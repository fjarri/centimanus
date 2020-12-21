import time
from centimanus.actor import ActorSystem


class MyRootActor:

    def __init__(self, this_actor):
        self.this_actor = this_actor
        self.r1 = this_actor.add_child(MyActor).get()
        self.r2 = this_actor.add_child(MyActor).get()

        self.this_actor.tell(1)
        print("Root started")

    def on_stop(self):
        print("Root stopping")

    def on_message(self, message):
        print("root message received:", message)
        if message == 1:
            r = self.r1.ask(2)
            print("root response received:", r.get())
        elif message == 3:
            r = self.r2.ask(4)
            print("root response received:", r.get())
        elif message == 0:
            self.this_actor.terminate()


class MyActor:

    def __init__(self, this_actor):
        print("Child starting")
        self.this_actor = this_actor

    def on_stop(self):
        print("Child stopping")

    def on_message(self, message):
        print("child message received:", message)
        if message == 2:
            self.this_actor.parent.tell(3)
            return 22
        elif message == 4:
            self.this_actor.parent.tell(0)
            return 44


root = ActorSystem(MyRootActor)
root.run_forever()
