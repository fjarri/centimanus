"""
Actor's rights:

Self:
- ask/tell
- stop/restart
- register child
- get parent handle

Parent:
- ask/tell

Child:
- ask/tell
- stop/restart
"""



import bisect
import threading
import queue
import time
import uuid

from .future import Future
from .channel import Channel


class SystemMessage:
    pass


class ActorRestart(SystemMessage):
    pass


# TODO: may even need a higher priority than other system messages
class ActorTerminate(SystemMessage):
    pass


class Envelope:

    def __init__(self, message, reply_to=None, delay=0):
        self.message = message
        self.reply_to = reply_to
        self.timestamp = time.time() + delay
        self.priority = 0 if isinstance(message, SystemMessage) else 1


class TimedInbox:
    def __init__(self):
        self.keys = []
        self.envelopes = []

    def empty(self):
        return len(self.keys) == 0

    def add(self, envelope):
        key = (envelope.priority, envelope.timestamp)
        idx = bisect.bisect(self.keys, key)
        self.keys.insert(idx, key)
        self.envelopes.insert(idx, envelope)

    def pop(self):
        self.keys.pop(0)
        return self.envelopes.pop(0)

    def next_event_in(self):
        if len(self.keys) == 0:
            return None
        else:
            _priority, timestamp = self.keys[0]
            return max(timestamp - time.time(), 0)


class InputChannel:

    def put(self):
        raise NotImplementedError


class OutputChannel:

    def empty_error(self):
        raise NotImplementedError

    def get(self, timeout=None):
        raise NotImplementedError


class MessagingHandle:

    def __init__(self, channel):
        self._channel = channel

    def tell(self, message):
        self._channel.put(Envelope(message))

    def ask(self, message):
        future = Future()
        self._channel.put(Envelope(message, reply_to=future))
        return future

    def messaging_handle(self):
        return MessagingHandle(self._channel)


class ControlHandle(MessagingHandle):

    def restart(self):
        return self.ask(ActorRestart())


class ChildHandle(ControlHandle):

    def __init__(self, launcher, channel):
        super().__init__(channel)
        self._launcher = launcher

    def terminate(self):
        self.tell(ActorTerminate())
        self._launcher.wait()


class SelfHandle(ControlHandle):

    def __init__(self, console, channel, parent_handle):
        super().__init__(channel)
        self._parent = parent_handle
        self._console = console

    def add_child(self, actor_factory):
        return self._console.add_child(actor_factory)

    def terminate(self):
        self.tell(ActorTerminate())

    @property
    def parent(self):
        return self._parent


class ActorSystem:

    def __init__(self, actor_factory):
        self._root_channel = Channel()
        self._launcher = Launcher(actor_factory, self._root_channel, parent_handle=None)

    def start(self):
        return self._launcher.start()

    def wait(self):
        self._launcher.wait()

    def run_forever(self):
        self._launcher.start().get()
        # TODO: process keyboard interrupt here?
        self._launcher.wait()


class Launcher:

    def __init__(self, actor_factory, channel, parent_handle=None):
        self._service_channel = Channel()
        self._thread = threading.Thread(
            target=event_loop,
            args=(self._service_channel, channel, actor_factory, parent_handle))

    def start(self):
        self._thread.start()
        future = self._service_channel.get()
        return future

    def wait(self):
        self._thread.join()


def event_loop(service_channel, channel, actor_factory, parent_handle):
    future = Future()
    service_channel.put(future)

    try:
        console = Console(channel, actor_factory, parent_handle)
        future.set(None)
    except Exception as e:
        future.set_exception()
        return

    console.event_loop(future)


class Console:

    class TerminateEventLoop(Exception):
        pass

    def __init__(self, channel, actor_factory, parent_handle):
        self.actor_factory = actor_factory
        self.parent_handle = parent_handle

        self._channel = channel

        self._children_launchers = {}
        self._children_channels = {}
        self._children_handles = {}

        self._self_handle = SelfHandle(self, self._channel, parent_handle) # TODO: weakref?
        self._actor = actor_factory(self._self_handle)

    def add_child(self, actor_factory):
        child_id = uuid.uuid1()
        channel = Channel()
        launcher = Launcher(actor_factory, channel, parent_handle=self._self_handle.messaging_handle())

        self._children_launchers[child_id] = launcher

        # need it to lock on termination
        # yes, the parent won't use it if it's terminating,
        # but it might have been passed as MessagingHandle to someone out of hierarchy
        self._children_channels[child_id] = channel
        child_handle = ChildHandle(launcher, channel)
        self._children_handles[child_id] = child_handle

        future = launcher.start()
        return future.map(lambda _: child_handle)

    def _terminate_children(self):
        child_ids = list(self._children_handles)
        for child_id in child_ids:
            self._children_handles[child_id].terminate()
            del self._children_handles[child_id]
            del self._children_launchers[child_id]
            del self._children_channels[child_id]

    def _process_envelope(self, envelope):
        if isinstance(envelope.message, ActorTerminate):
            self._terminate_children()
            raise self.TerminateEventLoop

        elif isinstance(envelope.message, ActorRestart):
            self._terminate_children()
            self._actor = self.actor_factory(self._self_handle) # TODO: process errors

        else:
            return self._actor.on_message(envelope.message)

    def event_loop(self, future):

        timed_inbox = TimedInbox()

        while True:

            next_event_in = timed_inbox.next_event_in()

            # Take all the messages out of the inbox and put them
            # in our internal inbox where they're sorted by timestamps
            try:
                envelope = self._channel.get(timeout=next_event_in)
                timed_inbox.add(envelope)
            except queue.Empty:
                # Raised if we waited for a non-None timeout,
                # but the queue is still empty.
                # This implies that there was something in the internal inbox
                # that is now ready to be processed.
                pass
            else:
                while not self._channel.empty():
                    envelope = self._channel.get()
                    timed_inbox.add(envelope)

                # Update the time of the next event,
                # since we received at least one new envelope.
                next_event_in = timed_inbox.next_event_in()

            # Check if there's something to be processed right now.
            if next_event_in > 0:
                continue

            envelope = timed_inbox.pop()

            try:
                result = self._process_envelope(envelope)
            except self.TerminateEventLoop:
                break
            except Exception as e:
                if envelope.reply_to is not None:
                    envelope.reply_to.set_exception()
            except BaseException:
                # TODO: should we terminate everything here?
                if envelope.reply_to is not None:
                    envelope.reply_to.set_exception()

            if envelope.reply_to is not None:
                envelope.reply_to.set(result)

        while not self._channel.empty():
            envelope = self._channel.get()
            timed_inbox.add(envelope)

        while not timed_inbox.empty():
            envelope = timed_inbox.pop()
            if envelope.reply_to is not None:
                # TODO: return an error, unless it's another stop message
                pass
