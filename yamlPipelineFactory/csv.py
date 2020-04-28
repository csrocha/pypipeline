from .yaml import Loader, YAMLObject
import csv


class CsvReader(YAMLObject):
    yaml_loader = Loader
    yaml_tag = u"!CsvReader"

    @classmethod
    def from_yaml(cls, loader, node):
        data = loader.construct_mapping(node)
        return cls(filename=data['filename'])

    def __init__(self, filename=None):
        self._filename = filename

    def __repr__(self):
        return f"{self.__class__.__name__}(filename={self._filename})"

    def run(self):
        with open(self._filename) as fd:
            for row in csv.reader(fd):
                yield row


class CsvWriter(YAMLObject):
    yaml_loader = Loader
    yaml_tag = u"!CsvWriter"

    @classmethod
    def from_yaml(cls, loader, node):
        data = loader.construct_mapping(node)
        return cls(filename=data['filename'], source=data['source'])

    def __init__(self, filename=None, source=None):
        self._filename = filename
        self._source = source

    def __repr__(self):
        return f"{self.__class__.__name__}(source={self._source}, filename={self._filename})"

    def run(self):
        with open(self._filename, 'w') as fd:
            writer = csv.writer(fd)
            for d in self._source.run():
                writer.writerow(d)
