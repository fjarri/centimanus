import sys

from .channel import Channel


class FutureResult:

    def __init__(self, value=None, exc_info=None):
        self.value = value
        self.exc_info = exc_info


class Future:

    def __init__(self):
        self._channel = Channel()

    def get(self, timeout=None):
        result = self._channel.get(timeout=timeout)
        if result.exc_info is not None:
            (exc_type, exc_value, exc_traceback) = result.exc_info
            if exc_value is None:
                exc_value = exc_type()
            if exc_value.__traceback__ is not exc_traceback:
                raise exc_value.with_traceback(exc_traceback)
            raise exc_value
        else:
            return result.value

    def set(self, value):
        self._channel.put(FutureResult(value=value))

    def set_exception(self, exc_info=None):
        assert exc_info is None or len(exc_info) == 3
        if exc_info is None:
            exc_info = sys.exc_info()
        self._channel.put(FutureResult(exc_info=exc_info))

    def map(self, func):
        return MappedFuture(self, func)


class MappedFuture:

    def __init__(self, future, func):
        self._future = future
        self._func = func

    def get(self, timeout=None):
        return self._func(self._future.get(timeout=timeout))
