import queue
import sys

from .channel import open_channel


class FutureResult:

    def __init__(self, value=None, exc_info=None):
        self.value = value
        self.exc_info = exc_info


class Future:

    def __init__(self, nursery):
        self._send_channel, self._receive_channel = open_channel(nursery)

    def __await__(self):
        result = yield from self._receive_channel.receive().__await__()
        if result.exc_info is not None:
            (exc_type, exc_value, exc_traceback) = result.exc_info
            if exc_value is None:
                exc_value = exc_type()
            if exc_value.__traceback__ is not exc_traceback:
                raise exc_value.with_traceback(exc_traceback)
            raise exc_value
        else:
            return result.value

    def set_external(self, value):
        # TODO: have two versions, async and not? Or use send_nowait()?
        self._send_channel.send_external(FutureResult(value=value))

    async def set(self, value):
        await self._send_channel.send(FutureResult(value=value))

    async def set_exception(self, exc_info=None):
        assert exc_info is None or len(exc_info) == 3
        if exc_info is None:
            exc_info = sys.exc_info()
        await self._send_channel.send(FutureResult(exc_info=exc_info))

    #def map(self, func):
    #    return MappedFuture(self, func)


class ThreadingFuture:

    def __init__(self):
        self._queue = queue.Queue()

    def get(self, timeout=None):
        result = self._queue.get(timeout=timeout)
        if result.exc_info is not None:
            (exc_type, exc_value, exc_traceback) = result.exc_info
            if exc_value is None:
                exc_value = exc_type()
            if exc_value.__traceback__ is not exc_traceback:
                raise exc_value.with_traceback(exc_traceback)
            raise exc_value
        else:
            return result.value

    def _result(self, value):
        return FutureResult(value=value)

    def _error(self, exc_info=None):
        assert exc_info is None or len(exc_info) == 3
        if exc_info is None:
            exc_info = sys.exc_info()
        return FutureResult(exc_info=exc_info)

    def set_sync(self, value):
        self._queue.put(self._result(value))

    def set_exception_sync(self, exc_info=None):
        self._queue.put(self._error(exc_info=exc_info))


class MappedFuture:

    def __init__(self, future, func):
        self._future = future
        self._func = func

    def get(self, timeout=None):
        return self._func(self._future.get(timeout=timeout))
