from assemply.node import node_class


@node_class('!StaticPusher')
class StaticPusher:
    """
    StaticPusher node descriptor. Send static data to the target queue defined.

    Source: [str]
    Target: [str]
    """
    def __init__(self, source=None, target=None):
        """
        Constructor of the StaticPusher.

        :param source:  List of data.
        :type source: List[Any]
        :param target: Target queue to push rows.
        :type target: Queue
        """
        self._source = source
        self._target = target

    def __repr__(self):
        """
        Representation of the StaticPusher node.

        :return: Formatted string.
        :rtype: str
        """
        return f"{self.__class__.__name__}(source={self._source})"

    async def run(self):
        """
        Loop execution to push data to the queue.

        :return: None
        :rtype: None
        """
        async with self._target as target:
            for data in self._source:
                await target.put(data)
