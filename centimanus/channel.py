import queue


class Channel:

    def __init__(self):
        self._queue = queue.Queue()

    def get(self, timeout=None):
        return self._queue.get(timeout=timeout)

    def empty(self):
        return self._queue.empty()

    def put(self, value):
        self._queue.put(value)
