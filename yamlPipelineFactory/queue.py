from yaml import MappingNode
from .yaml import add_constructor
from asyncio import Queue as AsyncQueue


class Queue:
    def __init__(self, *args, **kwargs):
        self._queue = AsyncQueue(*args, **kwargs)

    async def get(self):
        return await self._queue.get()

    async def put(self, data):
        return await self._queue.put(data)

    def task_done(self):
        self._queue.task_done()

    def __aiter__(self):
        return self

    async def __anext__(self):
        data = await self.get()
        if data is not StopAsyncIteration:
            self.task_done()
            return data
        else:
            raise StopAsyncIteration

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._queue.put(StopAsyncIteration)


def queue_constructor(loader, node):
    kwargs = loader.construct_mapping(node) if isinstance(node, MappingNode) else {}
    return Queue(**kwargs)


add_constructor("!Queue", queue_constructor)
