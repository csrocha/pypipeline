from yamlPipelineFactory import Builder, processor
import os


@processor(u'!MyNewProcessor')
class MyNewProcessor:
    """
    This is a user level class to Node.
    """
    def __init__(self, source=None, target=None):
        """
        Create a new node with this sources and target.

        :param source: Input Queue
        :type source: Queue
        :param target: Output Queue
        :type target: Queue
        """
        self._source = source
        self._target = target

    async def run(self):
        """
        Main execution.

        :return: None
        :rtype: None
        """
        async with self._target as target:
            async for data in self._source:
                await target.put([int(data[0])+int(data[1])])


def test_myprocessor_pipeline():
    """
    Test to create and execute a pipeline using the node class MyNewProcessor

    :return: --
    :rtype: --
    """
    with open("test_input.csv", "w") as infile:
        infile.write("1,2")

    yaml_string = """
--- !Pipeline
name: Test pipeline
nodes:
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
