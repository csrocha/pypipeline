from yamlPipelineFactory import Builder, processor
import os


@processor(u'!MyNewProcessor')
class MyNewProcessor:
    def __init__(self, source=None):
        self._source = source

    def run(self):
        for data in self._source.run():
            yield [int(data[0])+int(data[1])]


def test_null_pipeline():
    with open("test_input.csv", "w") as infile:
        infile.write("1,2")

    yaml_string = """
--- !Pipeline
    - !CsvReader &Input
        filename: test_input.csv
    - !MyNewProcessor &MyProcessor
        source: *Input
    - !CsvWriter &Output
        source: *MyProcessor
        filename: test_output.csv
"""
    builder = Builder(yaml_string)
    builder.run()

    with open("test_output.csv") as outfile:
        data = outfile.read()
        assert int(data) == 3

    os.remove("test_input.csv")
    os.remove("test_output.csv")