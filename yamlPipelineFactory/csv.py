from .processor import processor
import csv


@processor('!CsvReader')
class CsvReader:
    def __init__(self, filename=None, target=None):
        self._filename = filename
        self._target = target

    def __repr__(self):
        return f"{self.__class__.__name__}(filename={self._filename})"

    async def run(self):
        with open(self._filename) as fd:
            async with self._target as target:
                for row in csv.reader(fd):
                    await target.put(row)


@processor('!CsvWriter')
class CsvWriter:
    def __init__(self, filename=None, source=None):
        self._filename = filename
        self._source = source

    def __repr__(self):
        return f"{self.__class__.__name__}(source={self._source}, filename={self._filename})"

    async def run(self):
        with open(self._filename, 'w') as fd:
            writer = csv.writer(fd)

            async for data in self._source:
                writer.writerow(data)
