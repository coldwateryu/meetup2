import asyncio
import json

import aioamqp




class TcpClient(object):
    def __init__(self, pingpong_interval, loop):
        self.pingpong_interval = pingpong_interval
        self.reader = None
        self.writer = None
        self.tcp_received_message = None
        self.waiter = asyncio.Event()
        self.loop = loop
        self.loop.set_debug(enabled=True)
        self.start()

    async def get_client(self):
            try:
                self.reader, self.writer = await asyncio.open_connection('127.0.0.1', 10000, loop=self.loop)
                print('Connection established at "localhost:10000"')
            except OSError:
                print("Server not up retrying in 5 seconds...")
                await asyncio.sleep(5)

    def start(self):
        self.loop.run_until_complete(self.get_client())
        self.loop.run_until_complete(self.pingpong())
        msg = self.loop.run_until_complete(self.logon('hello'))

        if not msg == 'hello':
            print('Logon unsuccessful, closing connection with server...')
            self.writer.close()
            self.loop.close()
        else:
            print('Logon successful')

    async def call(self, request):
        if not self.writer:
            await self.get_client()
        self.writer.write(request.encode('utf-8'))
        await self.writer.drain()
        await self.read()
        await self.waiter.wait()

        return self.tcp_received_message

    async def read(self):
        data = await self.reader.read(4096)
        self.tcp_received_message = data.decode()
        self.waiter.set()

    async def logon(self, msg):
        print('Sending message:', msg)
        self.writer.write(msg.encode())
        data = await self.reader.read(15)
        resp = data.decode()
        print('Data received:', resp)
        return resp

    async def pingpong(self):
        while True:
            self.writer.write('PING'.encode())
            data = await self.reader.read(100)
            response = data.decode()
            print('Received:', response)
            if response == 'PONG':
                await asyncio.sleep(self.pingpong_interval)
                continue
            print('fuck this no pong on my ping')
            await self.get_client()


async def on_request(channel, body, envelope, properties):
    global tcp_client
    request = json.loads(body.decode('utf-8'))
    print(" [.] fib(%s)" % request)
    response = await tcp_client.call(request)

    if response:
        await channel.basic_publish(
            payload=str(response),
            exchange_name='',
            routing_key=properties.reply_to,
            properties={
                'correlation_id': properties.correlation_id,
            },
        )

        await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

    else:
        await tcp_client.connect()


@asyncio.coroutine
def rpc_server():
    global tcp_client

    transport, protocol = yield from aioamqp.connect()

    channel = yield from protocol.channel()

    yield from channel.queue_declare(queue_name='rpc_queue')
    yield from channel.basic_qos(prefetch_count=1, prefetch_size=0, connection_global=False)
    yield from channel.basic_consume(on_request, queue_name='rpc_queue')
    print(" [x] Awaiting RPC requests")


event_loop = asyncio.get_event_loop()
tcp_client = TcpClient(5, event_loop)
event_loop.run_until_complete(rpc_server())


try:
    event_loop.run_forever()
except KeyboardInterrupt:
    pass

event_loop.close()
