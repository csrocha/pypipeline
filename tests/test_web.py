from assemply import QueueNode, HTTPServerNode
import pytest
import asyncio
import logging

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("asyncio").setLevel(logging.WARNING)


async def client_request():
    reader, writer = await asyncio.open_connection('localhost', 8080)
    logging.debug("Client writing request")
    writer.write(b'GET / HTTP/1.0\r\nHost: www.any.com\r\n\r\nBody\r\n')
    await writer.drain()

    logging.debug("Client reading response")
    response = await reader.read(100)
    logging.debug(f"Client received {response}")

    logging.debug("Client closing connection")
    writer.close()
    await writer.wait_closed()

    logging.debug("Client finishing")
    return response


async def server_response(request_queue, response_queue):
    logging.debug("Server waiting for request")
    request = await request_queue.get()
    logging.debug(f"Server received request: {request}")

    async for data in request_queue.only({'id': request['id']}):
        logging.debug(f"Server received data: {data}")
        async with response_queue as response:
            logging.debug("Answering with 'No data'")
            await response.put({'id': data['id'], 'data': b'No data'})

    logging.debug("Server finishing")
    return request


@pytest.mark.asyncio
async def test_web_pipeline():
    """
    Test to create and execute a web pipeline.

    :return: --
    :rtype: --
    """
    request_queue = QueueNode()
    response_queue = QueueNode()
    monitor_queue = QueueNode()

    logging.debug("Creating server")
    server = HTTPServerNode(host='localhost', port=8080,
                            request=request_queue, response=response_queue, monitor=monitor_queue)

    server_task = asyncio.create_task(server.run())

    await monitor_queue.wait(lambda event: event.get('event_type') == 'started')

    logging.debug("Starting client and server processors")
    request, response = await asyncio.gather(server_response(request_queue, response_queue), client_request())

    logging.debug("Checking request message")
    del request['id']
    assert request == {'method': 'GET', 'uri': '/', 'version': 'HTTP/1.0', 'headers': {'Host': ' www.any.com'}}

    logging.debug("Checking response message")
    assert response == b'No data'

    logging.debug("Canceling server")
    server_task.cancel()

    try:
        await server_task
    except asyncio.CancelledError:
        print("main(): cancel_me is cancelled now")


@pytest.mark.asyncio
async def test_web_parse_request():
    """
    Test request parser.

    :return: --
    :rtype: --
    """
    pass


@pytest.mark.asyncio
async def test_web_parse_header():
    """
    Test request header parser.

    :return: --
    :rtype: --
    """
    pass


@pytest.mark.asyncio
async def test_web_push_body():
    """
    Test request body pusher.

    :return: --
    :rtype: --
    """
    pass

