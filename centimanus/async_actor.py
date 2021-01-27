import time
import bisect
import queue
import threading
import uuid
import trio

from .channel import open_channel
from .future import Future, ThreadingFuture


class TimedInbox:
    def __init__(self):
        self.keys = []
        self.envelopes = []

    def empty(self):
        return len(self.keys) == 0

    def add(self, envelope):
        priority = 0 if envelope.target is None else 1
        key = (envelope.timestamp, priority)
        idx = bisect.bisect(self.keys, key)
        self.keys.insert(idx, key)
        self.envelopes.insert(idx, envelope)

    def pop(self, ignore_targets):
        for i in range(len(self.envelopes)):
            if self.envelopes[i].target not in ignore_targets:
                self.keys.pop(i)
                return self.envelopes.pop(i)

    def next_event_in(self, ignore_targets):

        if len(self.keys) == 0:
            return None

        # TODO: we can maintain a dict of actor -> next message to speed it up
        for envelope in self.envelopes:
            if envelope.target not in ignore_targets:
                return envelope.timestamp - time.monotonic()

        return None

    async def read_channel(self, channel, ignore_targets):

        next_event_in = self.next_event_in(ignore_targets)

        while True:

            envelope = None
            if next_event_in is None:
                envelope = await channel.receive()
            elif next_event_in > 0:
                with trio.move_on_after(next_event_in):
                    envelope = await channel.receive()

            if envelope is not None:
                self.add(envelope)

            # Clean out the channel
            # TODO: put a marker in it first, so that we're not stuck cleaning it out indefinitely,
            # and instead take all the messages up to the marker.
            while True:
                try:
                    envelope = channel.receive_nowait()
                except trio.WouldBlock:
                    break

                self.add(envelope)

            next_event_in = self.next_event_in(ignore_targets)
            if next_event_in is not None and next_event_in <= 0:
                break

        # At this point there is something in the inbox to be processed right now,
        # and with the target not in `ignored_targets`.

        return self.pop(ignore_targets)


class Envelope:

    def __init__(self, message, target=None, reply_to=None, delay=0):
        self.message = message
        self.target = target
        self.reply_to = reply_to
        self.timestamp = time.monotonic() + delay


class AddActor:

    def __init__(self, parent_handle, actor_factory):
        self.parent_handle = parent_handle
        self.actor_factory = actor_factory

    def __str__(self):
        return f"AddActor({self.parent_handle}, {self.actor_factory})"


class TerminateActor:

    def __init__(self, id):
        self.id = id

    def __str__(self):
        return f"TerminateActor({self.id})"


class TerminateEventLoop:

    def __str__(self):
        return f"TerminateEventLoop()"


class ParentHandle:

    def __init__(self, id, nursery, channel):
        self.id = id
        self._nursery = nursery
        self._channel = channel

    def tell(self, value):
        self._channel.send_nowait(Envelope(value, target=self.id))

    def __str__(self):
        return f"ParentHandle({self.id})"


class ChildHandle:

    def __init__(self, id, nursery, channel):
        self.id = id
        self._nursery = nursery
        self._channel = channel

    async def ask(self, value):
        future = Future(self._nursery)
        await self._channel.send(Envelope(value, target=self.id, reply_to=future))
        return await future

    def __str__(self):
        return f"ChildHandle({self.id})"


class SelfHandle:

    def __init__(self, id, nursery, channel, parent_handle):
        self.id = id
        self._nursery = nursery
        self._channel = channel
        self._parent = parent_handle

        self._as_parent = ParentHandle(self.id, self._nursery, self._channel)

    async def add_child(self, actor_factory):
        future = Future(self._nursery)
        await self._channel.send(Envelope(AddActor(self._as_parent, actor_factory), target=None, reply_to=future))
        actor_id = await future
        return actor_id

    def terminate(self):
        self._channel.send_nowait(Envelope(TerminateActor(self.id)))

    def tell(self, value):
        self._channel.send_nowait(Envelope(value, target=self.id))

    @property
    def parent(self):
        return self._parent

    def __str__(self):
        return f"SelfHandle({self.id})"


class EventLoop:

    class Terminate(Exception):
        pass

    def __init__(self):
        self.actors = {} # actor id -> actor object
        self.children = {}

    def add_actor(self, parent_handle, actor_factory):
        root = parent_handle is None or parent_handle.id not in self.actors
        actor_id = uuid.uuid4()

        print(f"Asked to create {actor_factory} by {parent_handle}")
        print(f"  New id: {actor_id}")

        self_handle = SelfHandle(actor_id, self._nursery, self._channel, parent_handle)
        actor = actor_factory(self_handle)

        self.actors[actor_id] = actor
        if parent_handle is not None:
            if parent_handle.id not in self.children:
                self.children[parent_handle.id] = set()
            self.children[parent_handle.id].add(actor_id)

        print(f"  New children: {self.children}")

        child_handle = ChildHandle(actor_id, self._nursery, self._channel)
        return child_handle

    async def terminate_actor(self, id):

        # TODO: we can cancel all currently processed messages for this actor here,
        # and mark it as terminating so that new messages fail right away

        # TODO: call .on_stop() here?

        print(f"Asked to terminate {id}:\n  actors: {self.actors}\n  children: {self.children}")

        if id in self.children:
            for child_id in self.children[id]:
                print(f"Terminating child {child_id}")
                f = Future(self._nursery)
                await self._channel.send(Envelope(TerminateActor(child_id), reply_to=f))
                await f

            del self.children[id]

        del self.actors[id]

    async def process_message(self, envelope):
        if envelope.target is None:
            if isinstance(envelope.message, AddActor):
                return self.add_actor(envelope.message.parent_handle, envelope.message.actor_factory)
            elif isinstance(envelope.message, TerminateActor):
                await self.terminate_actor(envelope.message.id)
            elif isinstance(envelope.message, TerminateEventLoop):
                self._nursery.cancel_scope.cancel()
        else:
            return await self.actors[envelope.target].on_message(envelope.message)

    async def run_async(self, service_channel):

        active_actors = set()
        inbox = TimedInbox()

        async with trio.open_nursery() as nursery:

            send_channel, receive_channel = open_channel(nursery)

            self._channel = send_channel # TODO: find a better way to expose it
            self._nursery = nursery

            service_channel.put(send_channel)

            while True:

                with trio.CancelScope() as channel_cancel_scope:
                    envelope = await inbox.read_channel(receive_channel, set(active_actors))

                # if we're here, it's one of:
                # - there's a message to process
                # - an actor is finished and cancelled it

                if nursery.cancel_scope.cancelled_caught:
                    break

                if channel_cancel_scope.cancelled_caught:
                    # start waiting for the next event with the updated set of active actors
                    continue

                async def process_message(envelope):
                    try:
                        result = await self.process_message(envelope)
                        print(f"Processed {envelope.message}: {result}")

                        if envelope.reply_to is not None:
                            if isinstance(envelope.reply_to, Future):
                                await envelope.reply_to.set(result)
                            else:
                                envelope.reply_to.set_sync(result)

                    except Exception as e:
                        print(f"Processed {envelope.message}: failed with {e}")
                        if envelope.reply_to is not None:
                            if isinstance(envelope.reply_to, Future):
                                await envelope.reply_to.set_exception()
                            else:
                                envelope.reply_to.set_exception_sync()

                    if envelope.target is not None:
                        active_actors.remove(envelope.target)
                        channel_cancel_scope.cancel()

                print("Event loop got message", envelope.message)

                if envelope.target is not None:
                    active_actors.add(envelope.target)

                nursery.start_soon(process_message, envelope)

            # TODO: here we would lock the channel, respond to late messages etc

    def run(self, service_channel):
        trio.run(self.run_async, service_channel)


# TODO: add a single-thread option
def run_in_thread(actor_factory):
    service_channel = queue.Queue()
    thread = threading.Thread(target=event_loop, args=(service_channel,))
    thread.start()
    send_channel = service_channel.get()

    future = ThreadingFuture()
    send_channel.send_external(Envelope(AddActor(None, actor_factory), target=None, reply_to=future))
    child_handle = future.get()

    return RootHandle(thread, send_channel, child_handle.id)


class RootHandle:

    def __init__(self, thread, send_channel, root_actor_id):
        self._thread = thread
        self._send_channel = send_channel
        self._root_actor_id = root_actor_id

    def _send(self, obj):
        self._send_channel.send_external(obj)

    def tell(self, message):
        self._send(Envelope(message, target=self._root_actor_id))

    def ask(self, message):
        future = Future()
        self._send(Envelope(message, target=self._root_actor_id, reply_to=future))
        return future

    def terminate_event_loop(self):
        self._send(Envelope(TerminateEventLoop()))

    def wait(self):
        # TODO: process keyboard interrupt here?
        self._thread.join()


def event_loop(service_channel):
    event_loop = EventLoop()
    event_loop.run(service_channel)
