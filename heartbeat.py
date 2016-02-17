import asyncio


class StreamClient:
    def __init__(self, heartbeat_int, loop=None):
        self.heartbeat_int = heartbeat_int

        if loop is not None:
            self.loop = loop
        else:
            self.loop = asyncio.get_event_loop()

        self.loop.set_debug(enabled=True)

    def start(self):
        """ start manages the event loop, but it is not a coroutine """

        self.loop.run_until_complete(self.get_client())
        self.loop.create_task(self.start_timed_session())
        msg = self.loop.run_until_complete(self.logon('hello'))

        if msg == 'hello':
            try:
                self.loop.run_forever()
            except KeyboardInterrupt:
                print('Closing connection with server...')
                self.writer.close()
                self.loop.close()
        else:
            print('Logon unsuccessful, closing connection with server...')
            self.writer.close()
            self.loop.close()

    @asyncio.coroutine
    def get_client(self):
        self.reader, self.writer = yield from asyncio.open_connection(
            '127.0.0.1',
            10000,
            loop=self.loop
        )
        print('Connection established at "localhost:10000"')

    @asyncio.coroutine
    def timed_session(self):
        yield from asyncio.sleep(self.heartbeat_int)
        self.loop.create_task(self.start_timed_session())

    @asyncio.coroutine
    def start_timed_session(self):
        heartbeat_task = self.loop.create_task(self.timed_session())
        heartbeat_task.add_done_callback(self.heartbeat)

    @asyncio.coroutine
    def logon(self, msg):
        print('Sending message:', msg)
        self.writer.write(msg.encode())
        data = yield from self.reader.read(15)
        resp = data.decode()
        print('Data received:', resp)
        return resp

    def heartbeat(self, fut):
        """
        This is future's callback:
            1) Can't be a coroutine
            2) Takes a future as an argument
        """

        print('> Sending heartbeat...')
        self.writer.write('heartbeat'.encode())


# Start the client

client = StreamClient(5)
client.start()
