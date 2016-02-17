"""
    RPC server, aioamqp implementation of RPC examples from RabbitMQ tutorial
"""

import asyncio
import functools
import inspect
import json
from collections import deque

import aioamqp


@functools.lru_cache(None)
def fib(n):
    try:
        n = int(n)
    except ValueError:
        return b'only numbers, try again' + b'\r\n'

    if n < 2:
        return n
    return fib(n-1) + fib(n-2)


class FibonacciClient(asyncio.Protocol):
    @asyncio.coroutine
    def slow_fib(self, x):
        yield from asyncio.sleep(2)
        return fib(x)

    def fast_fib(self, x):
        return fib(x)

    def consume(self):
        while True:
            self.waiter = asyncio.Future()
            yield from self.waiter
            while len(self.receive_queue):
                data = self.receive_queue.popleft()
                if self.transport:
                    try:
                        res = self.process(data)
                        if isinstance(res, asyncio.Future) or inspect.isgenerator(res):
                            res = yield from res
                    except Exception as e:
                        print(e)

    def connection_made(self, transport):
        self.transport = transport
        self.receive_queue = deque()
        asyncio.Task(self.consume())

    def data_received(self, data):
        self.receive_queue.append(data)
        if not self.waiter.done():
            self.waiter.set_result(None)
        print("data_received {} {}".format(len(data), len(self.receive_queue)))

    def process(self, data):
        x = json.loads(data.decode())
        # res = self.fast_fib(x)
        res = yield from self.slow_fib(x)
        self.transport.write(json.dumps(res).encode('utf8'))
        # self.transport.close()

    def connection_lost(self, exc):
        self.transport = None

    @asyncio.coroutine
    def on_request(self, channel, body, envelope, properties):
        n = int(body)

        print(" [.] fib(%s)" % n)
        response = self.fast_fib(n)

        yield from channel.basic_publish(
            payload=str(response),
            exchange_name='',
            routing_key=properties.reply_to,
            properties={
                'correlation_id': properties.correlation_id,
            },
        )
    
        yield from channel.basic_client_ack(delivery_tag=envelope.delivery_tag)


@asyncio.coroutine
def rpc_server():
    global fib_client
    transport, protocol = yield from aioamqp.connect()

    channel = yield from protocol.channel()

    yield from channel.queue_declare(queue_name='rpc_queue')
    yield from channel.basic_qos(prefetch_count=1, prefetch_size=0, connection_global=False)
    yield from channel.basic_consume(fib_client.on_request, queue_name='rpc_queue')
    print(" [x] Awaiting RPC requests")


event_loop = asyncio.get_event_loop()
event_loop.set_debug(enabled=True)
fib_client = event_loop.create_server(FibonacciClient, '127.0.0.1', 10000)
client = event_loop.run_until_complete(fib_client)
server = event_loop.run_until_complete(rpc_server())
event_loop.run_forever()


try:
    event_loop.run_forever()
except KeyboardInterrupt:
    print("exit")
finally:
    client.close()
    server.close()
    event_loop.close()
