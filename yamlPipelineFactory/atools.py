from .exceptions import StopBucket


async def azip(*sources):
    """
    Asynchronous zip iterator.

    :param sources: List of asynchronous iterators.
    :type sources: [async iterators]
    :return: Generator of tuples
    :rtype: ((sources))
    """

    source_states = [True] * len(sources)

    while all(source_states):
        data = [await source.get() if enabled else StopBucket
                for source, enabled in zip(sources, source_states)]
        source_states = [value is not StopBucket for value in data]

        if all(source_states):
            yield tuple(data)
