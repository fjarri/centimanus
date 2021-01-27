import time
from centimanus.async_actor import run_in_thread


class MyRootActor:

    def __init__(self, this_actor):
        self.this_actor = this_actor
        self.this_actor.tell(1)
        print("Root started")

    async def on_stop(self):
        print("Root stopping")

    async def on_message(self, message):
        print("root message received:", message)
        if message == 1:
            self.r1 = await self.this_actor.add_child(MyActor)
            self.r2 = await self.this_actor.add_child(MyActor)
            r = self.r1.ask(2) # returns a Future
            print("root response received:", await r)
        elif message == 3:
            r = self.r2.ask(4)
            print("root response received:", await r)
        elif message == 0:
            self.this_actor.terminate()


class MyActor:

    def __init__(self, this_actor):
        print("Child starting")
        self.this_actor = this_actor

    async def on_stop(self):
        print("Child stopping")

    async def on_message(self, message):
        print("child message received:", message)
        if message == 2:
            self.this_actor.parent.tell(3)
            return 22
        elif message == 4:
            self.this_actor.parent.tell(0)
            return 44


root = run_in_thread(MyRootActor)
time.sleep(1)
root.terminate_event_loop()
root.wait()
