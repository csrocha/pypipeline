import asyncio
import socket
from http import HTTPStatus
import logging
import uuid
import traceback

from assemply.node import node_class


class AsyncHTTPRequestHandler:
    def __init__(self, request_queue, response_queue, monitor_queue,
                 reader, writer):
        self._request_queue = request_queue
        self._response_queue = response_queue
        self._monitor_queue = monitor_queue
        self.reader = reader
        self.writer = writer
        self.raw_requestline = None
        self.request_method = None
        self.request_uri = None
        self.request_version = None
        self.headers = {}
        self.body = ''
        self.close_connection = False
        self.id = None

    async def send_error(self, code, message=None, explain=None):
        await self.push_error(code, message, explain)

    def log_error(self, message):
        """ The base class used log_error, but I want use push_error monitor method. """
        logging.error(f"RID:{self.id}:{message}")

    def log_debug(self, message):
        """ The base class used log_error, but I want use push_error monitor method. """
        logging.debug(f"RID:{self.id}:{message}")

    def log_warning(self, message):
        """ The base class used log_error, but I want use push_error monitor method. """
        logging.warning(f"RID:{self.id}:{message}")

    async def push_error(self, code, message=None, explain=None):
        await self._monitor_queue.put({'type': 'error',
                                       'code': code,
                                       'message': message,
                                       'explain': explain})

    async def parse_headers(self):
        self.headers = {}

        while True:
            self.raw_requestline = await self.reader.readline()
            line = self.raw_requestline[:-2].decode('ascii')
            if not line:
                return
            key, value = line.split(':')
            self.headers[key] = value

    async def push_request(self):
        self.log_debug("Pushing request.")
        await self._request_queue.put({'method': self.request_method,
                                       'uri': self.request_uri,
                                       'version': self.request_version,
                                       'headers': self.headers,
                                       'id': self.id.urn})

    async def push_body(self, encoding='ascii'):
        i = 0

        async with self._request_queue as request:
            while True:
                try:
                    self.raw_requestline = await self.reader.readline()
                except socket.error as e:
                    self.log_warning(f"Connection error: {e}")
                    return
                except Exception as e:
                    self.log_error(e)
                    traceback.print_exc()
                    return
                line = self.raw_requestline.decode(encoding)
                if not line:
                    return
                await request.put({'id': self.id.urn,
                                   'idx': i,
                                   'data': line})
                i += 1

    async def parse_request(self):
        # Parsing first request line
        request_method, request_uri, request_version = self.raw_requestline.split(b' ')

        self.request_method = request_method.decode('ascii')
        self.request_uri = request_uri.decode('ascii')  # Checking right decoding
        self.request_version = request_version[:-2].decode('ascii')

        await self.parse_headers()

        self.id = uuid.uuid5(uuid.NAMESPACE_URL, self.request_uri)

        await self.push_request()

        return True

    async def pop_response(self):
        # Must receive a proper request
        self.log_debug("Waiting for response.")
        async for response in self._response_queue:
            self.writer.write(response['data'])
            await self.writer.drain()
        self.writer.close()
        await self.writer.wait_closed()

    async def handle_one_request(self):
        try:
            self.raw_requestline = await self.reader.readline()
            if len(self.raw_requestline) > 65536:
                self.request_method = ''
                self.request_uri = ''
                self.request_version = ''
                await self.send_error(HTTPStatus.REQUEST_URI_TOO_LONG)
                return
            if not self.raw_requestline:
                self.close_connection = True
                return
            if not await self.parse_request():
                # An error code has been sent, just exit
                return

            self.log_debug("Starting body requester and responder.")
            await asyncio.gather(self.push_body(), self.pop_response())

        except socket.timeout as e:
            # Push error
            self.log_error(f"Request timed out: {e}")
            return

        except socket.error as e:
            # Push error
            self.log_error(f"Connection error: {e}")
            return

        except Exception as e:
            print("ouch!")
            print(e)
            traceback.print_exc()
            return

    async def handle(self):
        while not self.close_connection:
            await self.handle_one_request()


@node_class('!HTTPServer')
class HTTPServerNode:
    """HTTP Server node"""
    def __init__(self, host=None, port=None, request=None, response=None, monitor=None):
        """
        Endpoint.
        """
        self._request_queue = request
        self._response_queue = response
        self._monitor_queue = monitor
        self._host = host
        self._port = port
        self._server = None
        self._buffer_size = None
        self._request_handler = None

    async def serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        transport = getattr(reader, '_transport')
        if transport:
            # Could be useful: client_address, *_args = transport.get_extra_info('peername')
            handler = AsyncHTTPRequestHandler(self._request_queue, self._response_queue, self._monitor_queue,
                                              reader, writer)
            await handler.handle()
        else:
            raise ConnectionRefusedError

    async def run(self):
        """
        Loop execution to push data to the queue.

        :return: None
        :rtype: None
        """
        self._server = await asyncio.start_server(self.serve,
                                                  host=self._host,
                                                  port=self._port)
        async with self._server:
            await self._monitor_queue.put({'event_type': 'started'})
            await self._server.serve_forever()
