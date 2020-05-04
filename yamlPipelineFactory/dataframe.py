from .node import node_class
from .atools import azip
import pandas as pd

@node_class('!DataFrameBuilder')
class DataFrameBuilder:
    """
    Pandas dataframe builder node.
    """
    def __init__(self, source=None, target=None):
        """
        Create a dataframe builder node to build a dataframe getting rows from source.

        :param source: Source queue of rows.
        :type source: Queue of List of Any
        :param target: Target queue of dataframes.
        :type target: Queue of dataframe
        """
        self._source = source
        self._target = target

    def __repr__(self):
        """
        Representation of dataframe builder node.

        :return: Formatted node data
        :rtype: str
        """
        return f"{self.__class__.__name__}(source={self._source}, target={self._target})"

    async def run(self):
        """
        Loop execution to retrieve rows from the source queue to create
        a dataframe and then push it to the target queue.

        :return: Main node corutine
        :rtype: corutine
        """
        df = pd.DataFrame()
        async for data in self._source:
            df.append(data)

        async with self._target as target:
            target.put(df)


@node_class('!DataFrameRowSplit')
class DataFrameRowSplit:
    """
    Pandas dataframe row splitter node.
    """
    def __init__(self, source=None, target=None):
        """
        Constructor of dataframe splitter node. Take a dataframe from queue source and push each row on target queue.

        :param source: Source queue of dataframes
        :type source: Queue of Dataframes
        :param target: Target queue of rows.
        :type target: Queue of list of Any
        """
        self._source = source
        self._target = target

    def __repr__(self):
        """
        Representation of dataframe splitter node.

        :return: Formatted node data
        :rtype: str
        """
        return f"{self.__class__.__name__}(source={self._source}, target={self._target})"

    async def run(self):
        """
        Loop execution to retrieve dataframes from the source queue to push rows to the target queue.

        :return: Main node corutine
        :rtype: corutine
        """
        async with self._target as target:
            async for df in self._source:
                for index, row in df.iterrows():
                    target.put((index, row))

@node_class('!DataFrameJoin')
class DataFrameJoin:
    """
    Pandas dataframe joiner node.
    """
    def __init__(self, left=None, right=None, target=None, **kwargs):
        """
        Constructor of dataframe joiner node. Take two dataframes from left and right queues
        and push the joined dataframe to the target queue.

        :param left: Left dataframe queue
        :type left: Queue of dataframe
        :param right: Right dataframe queue
        :type right: Queue of dataframe
        :param target: Result of join left and right dataframe.
        :type target: Queue of dataframe
        :param kwargs: Join parameters.
        :type kwargs: dict of named parameters
        """
        self._left = left
        self._right = right
        self._target = target
        self._join_args = kwargs

    def __repr__(self):
        """
        Representation of dataframe joiner node.

        :return: Formatted node data
        :rtype: str
        """
        return f"{self.__class__.__name__}(left={self._left}, right={self._right}, target={self._target})"

    async def run(self):
        async with self._target as target:
            async for (ldf, rdf) in azip(self._left, self._right):
                ldf.join(rdf, **self._join_args)
