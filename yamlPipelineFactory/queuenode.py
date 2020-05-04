from yaml import MappingNode
from asyncio import Queue as AsyncQueue
from .yaml import add_constructor
from .exceptions import StopBucket, StopProcess


class QueueIterator:
    def __init__(self, queue, stop_exception, exit_exception):
        self._queue = queue
        self._stop_exception = stop_exception
        self._exit_exception = exit_exception

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

        Stops if data is StopBucket

        :return: Data
        :rtype: Any
        """
        data = await self._queue.get()
        if data is not self._stop_exception:
            self._queue.task_done()
            return data
        else:
            self._queue.task_done()
            raise StopAsyncIteration


class QueueContext:
    def __init__(self, queue, exit_exception):
        self._queue = queue
        self._exit_exception = exit_exception

    async def __aenter__(self):
        """
        Enter to runtime context to usa the queue as target

        :return: self
        :rtype: awaitable context
        """
        return self

    async def put(self, data):
        """
        Push data in the queue context

        :param data: Data to send to the queue
        :type data: Any
        :return: None
        :rtype: None
        """
        await self._queue.put(data)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Exit of the context and push StopBucket to communicate finish of communication.

        :param exc_type: Captured exception type
        :type exc_type: Exception
        :param exc_val: Captured exception value
        :type exc_val: Exception Instance
        :param exc_tb: Traceback object
        :type exc_tb: Traceback
        :return: None
        :rtype: None
        """
        await self.put(self._exit_exception)


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
        self._received = 0
        self._processed = 0
        self._delivered = 0

    def waiting(self):
        return self._received - self._processed

    def processing(self):
        return self._received - (self.waiting() + self.done())

    def done(self):
        return self._delivered - (self.processing() + self.waiting())

    def __repr__(self):
        return f"Queue Status -- received: {self._received} {self._delivered} {self._processed}"

    async def get(self):
        """
        Get data from the Queue

        :return: data
        :rtype: Any
        """
        self._delivered += 1
        return await self._queue.get()

    async def put(self, data):
        """
        Push data to the Queue

        :param data: data to push
        :type data: Any
        :return: None
        :rtype: None
        """
        self._received += 1
        return await self._queue.put(data)

    def task_done(self):
        """
        Indicate that a formerly enqueued task is complete.
        Check: https://docs.python.org/3/library/asyncio-queue.html#asyncio.Queue.task_done

        Raises ValueError if called more times than there were items placed in the queue.

        :return: None
        :rtype: None
        """
        self._processed += 1
        self._queue.task_done()

    def __aiter__(self):
        """
        Present itself a an asynchronous iterator.

        :return: self
        :rtype: asynchronous iterator
        """
        return QueueIterator(self, StopProcess, StopProcess)

    def iter(self, stop_exception, exit_exception=None):
        return QueueIterator(self, stop_exception, exit_exception=exit_exception or stop_exception)

    async def __aenter__(self):
        """
        Enter to runtime context to usa the queue as target

        :return: self
        :rtype: awaitable context
        """
        return QueueContext(self, StopProcess)

    def ctx(self, exit_exception):
        return QueueContext(self, exit_exception)


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
