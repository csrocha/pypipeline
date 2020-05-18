from yaml import MappingNode
from asyncio import Queue as AsyncQueue
from assemply.yaml import add_constructor
from assemply.exceptions import StopBucket


class QueueBaseIterator(object):
    """
    Abstract Iterator Queue.
    """
    def __init__(self, queue, __next_data__=None):
        """
        Create a base iterator queue.
        :param queue: low level Queue.
        :param __next_data__: Previous data taken from the queue.
        """
        self._queue = queue
        self._next_data_ = __next_data__
        self._delivered = False

    async def get(self):
        """
        Take data from the queue.

        :return: Data from the queue
        :rtype: Any
        """
        if self._delivered:
            self._queue.task_done()

        data = await self._queue.get() if self._next_data_ is None else self._next_data_
        self._next_data_ = None
        return data

    def __aiter__(self):
        """
        Present itself a an asynchronous iterator.

        :return: self
        :rtype: asynchronous iterator
        """
        return self

    def process(self, data):
        """
        Iterator processor

        :param data: Data to process.
        :type data: Any
        :return: Processed data or raise StopAsyncIterator
        :rtype: Any
        """
        raise NotImplementedError

    async def __anext__(self):
        """
        Returns the next value in the queue.

        Stops if data is StopBucket

        :return: Data
        :rtype: Any
        """
        return self.process(await self.get())


class QueueItemIterator(QueueBaseIterator):
    """
    Queue iterator by items in the queue.
    """
    def __init__(self, queue, __next_data__=None):
        super(QueueItemIterator, self).__init__(queue, __next_data__)

    def process(self, data):
        """
        Process the next data itself.

        :param data: Data from the queue.
        :type data: Any
        :return: Processed data from the queue or StopAsyncIteration
        :rtype: Any
        """
        self._delivered = not isinstance(data, StopBucket)

        if self._delivered:
            return data
        else:
            self._queue.task_done()
            raise StopAsyncIteration


class QueueBucketIterator(QueueBaseIterator):
    def __init__(self, queue, depth=1, level=None, __next_data__=None):
        super(QueueBucketIterator, self).__init__(queue, __next_data__)
        self._queue = queue
        self._depth = depth
        self._level = level if level is not None else depth
        self._next_data_ = __next_data__

    def process(self, data):
        """
        Process data from queue.

        :param data: Data from queue.
        :type data: Any
        :return: Data from queue or StopAsyncIteration
        :rtype: Any
        """
        if isinstance(data, StopBucket):
            if data.level == self._depth - self._level:
                raise StopAsyncIteration

        if self._level == 1:
            return QueueItemIterator(self._queue, __next_data__=data)
        else:
            return QueueBucketIterator(self._queue, self._depth, self._level - 1, __next_data__=data)


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
        self._context_depth = -1

    def waiting(self):
        return self._received - self._processed

    def processing(self):
        return self._received - (self.waiting() + self.done())

    def done(self):
        return self._delivered - (self.processing() + self.waiting())

    def assert_sanity(self):
        assert self._received >= self._delivered >= self._processed

    def __repr__(self):
        return f"Queue Status -- received: {self._received} {self._delivered} {self._processed}"

    async def get(self):
        """
        Get data from the Queue

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

    def empty(self):
        """
        Return True if the queue is empty, False otherwise.

        :return: True if the queue is empty, False otherwise.
        :rtype: bool
        """
        return self._queue.empty()

    def __aiter__(self):
        """
        Present itself a an asynchronous iterator.

        :return: self
        :rtype: asynchronous iterator
        """
        return QueueItemIterator(self)

    def buckets(self, depth=1):
        """
        Returns an asynchronous iterator of buckets.

        :return: Bucket iterator with depth depth.
        :rtype: asynchronous iterator
        """
        return QueueBucketIterator(self._queue, depth) if depth > 0 else QueueItemIterator(self)

    async def __aenter__(self):
        """
        Enter to runtime context to use the queue as target

        :return: self
        :rtype: awaitable context
        """
        self._context_depth += 1
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Exit of the context and push exit exception to communicate finish of communication.

        :param exc_type: Captured exception type
        :type exc_type: Exception
        :param exc_val: Captured exception value
        :type exc_val: Exception Instance
        :param exc_tb: Traceback object
        :type exc_tb: Traceback
        :return: None
        :rtype: None
        """
        await self.put(StopBucket(self._context_depth))
        self._context_depth -= 1


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
