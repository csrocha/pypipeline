from yaml import MappingNode
from .yaml import add_constructor
from asyncio import Queue as AsyncQueue


class QueueNode:
    """
    Queue node class to encapsulate async communication between process nodes.
    """

    def __init__(self, *args, **kwargs):
        """
        Create a new QueueNode with asyncio.Queue parameters.

        :param args: Arguments for asyncio.Queue
        :type args: list of arguments
        :param kwargs: Named parameters for asyncio.Queue
        :type kwargs: dict of named arguments
        """
        self._queue = AsyncQueue(*args, **kwargs)

    async def get(self):
        """
        Get data from the Queue

        :return: data
        :rtype: Any
        """
        return await self._queue.get()

    async def put(self, data):
        """
        Push data to the Queue

        :param data: data to push
        :type data: Any
        :return: None
        :rtype: None
        """

        return await self._queue.put(data)

    def task_done(self):
        """
        Indicate that a formerly enqueued task is complete.
        Check: https://docs.python.org/3/library/asyncio-queue.html#asyncio.Queue.task_done

        Raises ValueError if called more times than there were items placed in the queue.

        :return: None
        :rtype: None
        """
        self._queue.task_done()

    def __aiter__(self):
        """
        Present itself a an asynchronous iterator.

        :return: self
        :rtype: asynchronous iterator
        """
        return self

    async def __anext__(self):
        """
        Returns the next value in the Queue.

        Stops if data is StopAsyncIteration

        :return: Data
        :rtype: Any
        """
        data = await self.get()
        if data is not StopAsyncIteration:
            self.task_done()
            return data
        else:
            raise StopAsyncIteration

    async def __aenter__(self):
        """
        Enter to runtime context to usa the queue as target

        :return: self
        :rtype: awaitable context
        """
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Exit of the context and push StopAsyncIteration to communicate finish of communication.

        :param exc_type: Captured exception type
        :type exc_type: Exception
        :param exc_val: Captured exception value
        :type exc_val: Exception Instance
        :param exc_tb: Traceback object
        :type exc_tb: Traceback
        :return: None
        :rtype: None
        """
        await self._queue.put(StopAsyncIteration)


def queue_constructor(loader, node):
    """
    Constructor of the QueueNode instance from the YAML blueprint.

    :param loader: YAML loader
    :type loader: Loader
    :param node: Node with QueueNode information or description. Could be none.
    :type node: Node
    :return: new QueueNode instance.
    :rtype: QueueNode
    """
    kwargs = loader.construct_mapping(node) if isinstance(node, MappingNode) else {}
    return QueueNode(**kwargs)


# Add the tag Queue to the YAML blueprint interpreter
add_constructor("!Queue", queue_constructor)
