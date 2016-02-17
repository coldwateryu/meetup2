import asyncio
import json

import functools
import logging
import sys


logging.basicConfig(
    level=logging.DEBUG,
    format='%(name)s: %(message)s',
    stream=sys.stderr,
)
log = logging.getLogger('main')

event_loop = asyncio.get_event_loop()


@functools.lru_cache(None)
def fib(n):
    try:
        n = int(n)
    except ValueError:
        return b'only numbers, try again' + b'\r\n'

    if n < 2:
        return n
    return fib(n-1) + fib(n-2)


async def handle(reader, writer):
    while True:
        data = await reader.read(100)
        message = data.decode()
        addr = writer.get_extra_info('peername')
        print("Received %r from %r" % (message, addr))
        if message not in ['PING', 'hello']:
            try:
                message = int(message)
            except ValueError:
                try:
                    message = json.loads(message)
                except ValueError:
                    response = 'I only accept ints and regulation dicts'
        else:
            response = 'PONG' if message == 'PING' else 'hello'

        if isinstance(message, int):
            response = str(fib(int(message)))
        elif isinstance(message, dict):
            response = str(fib(int(message['payload'])))

        print("Send: %r" % response)
        writer.write(response.encode(encoding='utf-8'))
        await writer.drain()

    # print("Close the client socket")
    # writer.close()

loop = asyncio.get_event_loop()
loop.set_debug(enabled=True)
coro = asyncio.start_server(handle, '127.0.0.1', 10000, loop=loop)
server = loop.run_until_complete(coro)

# Serve requests until Ctrl+C is pressed
print('Serving on {}'.format(server.sockets[0].getsockname()))
try:
    loop.run_forever()
except KeyboardInterrupt:
    pass

# Close the server
server.close()
loop.run_until_complete(server.wait_closed())
loop.close()
