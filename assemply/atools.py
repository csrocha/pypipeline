import asyncio
from .exceptions import StopBucket


async def azip(*sources):
    """
    Asynchronous zip iterator.

    :param sources: List of asynchronous iterators.
    :type sources: [async iterators]
    :return: Generator of tuples
    :rtype: ((sources))
    """

    while True:
        data = await asyncio.gather(*[source.get() for source in sources])
        if any(isinstance(d, StopBucket) for d in data):
            break
        yield data
