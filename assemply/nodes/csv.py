from assemply.node import node_class
import csv


@node_class('!CsvReader')
class CsvReader:
    """
    CsvReader node descriptor. Read a csv file on filesystem and push each row to the pipeline.

    Filename: [str]
    Target: [str]
    """
    def __init__(self, filename=None, target=None, **kwargs):
        """
        Constructor of the CsvReader. Create a node to read an specific file and push rows to the target queue.

        :param filename:  Filename queue to take rows.
        :type filename: Queue
        :param target: Target queue to push rows.
        :type target: Queue
        :param kwargs: Parameters for cvs.reader functions
        :type kwargs: dict
        """
        self._filename = filename
        self._target = target
        self._reader_kwargs = kwargs

    def __repr__(self):
        """
        Representation of the CsvReader node.

        :return: Formatted string.
        :rtype: str
        """
        return f"{self.__class__.__name__}(filename={self._filename})"

    async def run(self):
        """
        Loop execution to retrieve rows from file and push to the queue.

        :return: None
        :rtype: None
        """
        async with self._target as main_target:
            async for filename in self._filename:
                with open(filename) as fd:
                    async with main_target as bucket_target:
                        for row in csv.reader(fd, **self._reader_kwargs):
                            await bucket_target.put(row)


@node_class('!CsvWriter')
class CsvWriter:
    """
    CsvWriter node descriptor. Write a csv file to filesystem taking each pushed message in a Queue.

    Input: [str compatible]
    No Output.
    """
    def __init__(self, filename=None, source=None, **kwargs):
        """
        Constructor of the CsvWriter. Create a node to write to an specific file taking rows from a source queue.

        :param filename:  Filename to store rows.
        :type filename: Queue
        :param source: Source queue to get rows.
        :type source: Queue
        :param kwargs: Parameters for cvs.writer functions
        :type kwargs: dict
        """
        self._filename = filename
        self._source = source
        self._writer_kwargs = kwargs

    def __repr__(self):
        """
        Representation of the CsvWriter node.

        :return: Formatted string.
        :rtype: str
        """
        return f"{self.__class__.__name__}(source={self._source}, filename={self._filename})"

    async def run(self):
        """
        Loop execution to write rows to file from the source queue.

        :return: None
        :rtype: None
        """
        async for filename in self._filename:
            with open(filename, 'w') as fd:
                writer = csv.writer(fd, **self._writer_kwargs)
                async for data in self._source:
                    writer.writerow(data)
