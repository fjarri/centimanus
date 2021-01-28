import time
import bisect
import queue
import threading
import uuid
import trio

from .channel import open_channel
from .future import Future, ThreadingFuture


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
        self.actor_channels = {} # actor id -> actor send channel
        self.actor_cancels = {}
        self.children = {}


    def add_actor(self, parent_handle, actor_factory):
        root = parent_handle is None or parent_handle.id not in self.actors
        actor_id = uuid.uuid4()

        print(f"Asked to create {actor_factory} by {parent_handle}")
        print(f"  New id: {actor_id}")

        self_handle = SelfHandle(actor_id, self._nursery, self._channel, parent_handle)
        actor = actor_factory(self_handle)

        self.actors[actor_id] = actor

        send_channel, receive_channel = trio.open_memory_channel(0)
        self.actor_channels[actor_id] = send_channel

        cancel_scope = trio.CancelScope()
        self.actor_cancels[actor_id] = cancel_scope

        if parent_handle is not None:
            if parent_handle.id not in self.children:
                self.children[parent_handle.id] = set()
            self.children[parent_handle.id].add(actor_id)

        print(f"  New children: {self.children}")

        self._nursery.start_soon(self.actor_loop, cancel_scope, receive_channel)

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

        print(f"Cancelling {id}")
        self.actor_cancels[id].cancel()
        del self.actors[id]

    async def on_message(self, message):
        if isinstance(message, AddActor):
            return self.add_actor(message.parent_handle, message.actor_factory)
        elif isinstance(message, TerminateActor):
            await self.terminate_actor(message.id)
        elif isinstance(message, TerminateEventLoop):
            self._nursery.cancel_scope.cancel()
        else:
            raise Exception(f"Unknown message type: {type(message)}")

    async def actor_loop(self, cancel_scope, receive_channel):

        with cancel_scope:
            while True:
                envelope = await receive_channel.receive()
                # TODO: process delay here
                # TODO: process timeout here
                await self.process_message_and_reply(self.actors[envelope.target].on_message, envelope)

    async def process_message_and_reply(self, handler, envelope):
        try:
            result = await handler(envelope.message)
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

    async def run_async(self, service_channel):

        async with trio.open_nursery() as nursery:

            send_channel, receive_channel = open_channel(nursery)

            self._channel = send_channel # TODO: find a better way to expose it
            self._nursery = nursery

            service_channel.put(send_channel)

            while True:
                envelope = await receive_channel.receive()
                if envelope.target is not None:
                    await self.actor_channels[envelope.target].send(envelope)
                else:
                    nursery.start_soon(self.process_message_and_reply, self.on_message, envelope)

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
