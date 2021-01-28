import trio


def open_channel(nursery):
    send_channel, receive_channel = trio.open_memory_channel(100)
    return SendChannel(send_channel, nursery), receive_channel


class SendChannel:

    def __init__(self, send_channel, nursery):
        self._token = trio.lowlevel.current_trio_token()
        self._nursery = nursery
        self._send_channel = send_channel

    async def send(self, message):
        if trio.lowlevel.current_trio_token() == self._token:
            await self._send_channel.send(message)
        else:
            self.send_external(message)

    def send_nowait(self, message):
        if trio.lowlevel.current_trio_token() == self._token:
            self._send_channel.send_nowait(message)
        else:
            self.send_external(message)

    def send_external(self, message):
        trio.from_thread.run_sync(self._nursery.start_soon, self._send_channel.send, message, trio_token=self._token)
