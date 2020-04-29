from .processor import processor
import csv


@processor('!CsvReader')
class CsvReader:
    """
    CsvReader node descriptor. Read a csv file on filesystem and push each row to the pipeline.

    No Source.
    Target: [str]
    """
    def __init__(self, filename=None, target=None, **kwargs):
        """
        Constructor of the CsvReader. Create a node to read an specific file and push rows to the target queue.

        :param filename:  Filename to take rows.
        :type filename: str
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
        with open(self._filename) as fd:
            async with self._target as target:
                for row in csv.reader(fd, **self._reader_kwargs):
                    await target.put(row)


@processor('!CsvWriter')
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
        :type filename: str
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
        with open(self._filename, 'w') as fd:
            writer = csv.writer(fd, **self._writer_kwargs)

            async for data in self._source:
                writer.writerow(data)
