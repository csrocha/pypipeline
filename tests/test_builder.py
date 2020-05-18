from assemply import PipelineBuilder, node_class
import os
import pytest


@node_class(u'!MyNewProcessor')
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
            async for bucket in self._source.buckets(1):
                async for data in bucket:
                    await target.put([int(data[0])+int(data[1])])


@pytest.mark.asyncio
async def test_myprocessor_pipeline(tmp_path):
    """
    Test to create and execute a pipeline using the node class MyNewProcessor

    :return: --
    :rtype: --
    """
    source_file = tmp_path / "test_input.csv"
    target_file = tmp_path / "test_output.csv"

    with open(source_file, "w") as infile:
        infile.write("1,2")

    yaml_string = f"""
--- !Pipeline
name: Test pipeline
nodes:
    - !StaticPusher
        target: !Queue &FileInput
        source:
        - {source_file}
    - !StaticPusher
        target: !Queue &FileOutput
        source:
        - {target_file}
    - !CsvReader
        filename: *FileInput
        target: !Queue &TestInput
    - !MyNewProcessor
        source: *TestInput
        target: !Queue &MyNewProcessorOutput
    - !CsvWriter
        source: *MyNewProcessorOutput
        filename: *FileOutput
"""
    builder = PipelineBuilder(yaml_string)
    pipeline = builder.build()

    await pipeline.run()

    with open(target_file) as outfile:
        data = outfile.read()
        assert int(data) == 3

    os.remove(source_file)
    os.remove(target_file)
