from assemply import QueueNode, RequestNode
import pytest
import asyncio
import logging

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("asyncio").setLevel(logging.WARNING)


@pytest.mark.asyncio
async def test_simple():
    request_queue = QueueNode()
    response_queue = QueueNode()
    exception_queue = QueueNode()

    request_node = RequestNode(request_queue, response_queue, exception_queue)

    async def print_responses(queue):
        async for data in queue:
            assert(data.json()[0]['name'] == 'Afghanistan')

    async def print_exceptions(queue):
        async for data in queue:
            print("OUCH! " + data)

    req = asyncio.create_task(request_node.run())
    res = asyncio.create_task(print_responses(response_queue))
    exc = asyncio.create_task(print_exceptions(exception_queue))

    async with request_queue as request:
        await request.put(('get', 'https://restcountries.eu/rest/v2/all', {}))

    await req
    await res
    await exc
