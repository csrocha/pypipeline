from yamlPipelineFactory import Builder, processor
import os


@processor(u'!MyNewProcessor')
class MyNewProcessor:
    def __init__(self, source=None, target=None):
        self._source = source
        self._target = target

    async def run(self):
        async with self._target as target:
            async for data in self._source:
                await target.put([int(data[0])+int(data[1])])


def test_null_pipeline():
    with open("test_input.csv", "w") as infile:
        infile.write("1,2")

    yaml_string = """
--- !Pipeline
name: Test pipeline
tasks:
    - !CsvReader &Input
        filename: test_input.csv
        target: !Queue &TestInput
    - !MyNewProcessor
        source: *TestInput
        target: !Queue &MyNewProcessorOutput
    - !CsvWriter
        source: *MyNewProcessorOutput
        filename: test_output.csv
"""
    builder = Builder(yaml_string)
    builder.run()

    with open("test_output.csv") as outfile:
        data = outfile.read()
        assert int(data) == 3

    os.remove("test_input.csv")
    os.remove("test_output.csv")
