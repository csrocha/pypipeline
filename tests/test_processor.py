from assemply import PipelineBuilder, node_sub
import os
import pytest
from assemply.exceptions import StopProcess


@node_sub("!Test_process", inputs=("source",), outputs=("target",))
def node_test(source):
    return [int(source[0])**int(source[1])]


@pytest.mark.asyncio
async def test_nodesub(event_loop, tmp_path):
    """
    """
    source_file = tmp_path / "test_input.csv"
    target_file = tmp_path / "test_output.csv"

    with open(source_file, "w") as infile:
        infile.write("1,2")

    yaml_string = f"""
--- !Pipeline
name: Test pipeline
nodes:
    - !CsvReader &Input
        filename: !Queue &FileInput
        target: !Queue &TestInput
    - !Test_process
        source: *TestInput
        target: !Queue &MyNewProcessorOutput
    - !CsvWriter
        source: *MyNewProcessorOutput
        filename: !Queue &FileOutput
expose:
    file_input: *FileInput
    file_output: *FileOutput
"""
    builder = PipelineBuilder(yaml_string)
    pipeline = builder.build()

    async with pipeline as p:
        await p.file_input.put(source_file)
        await p.file_output.put(target_file)
        await p.file_input.put(StopProcess())
        await p.file_output.put(StopProcess())

    with open(target_file) as outfile:
        data = outfile.read()
        assert int(data) == 1

    os.remove(source_file)
    os.remove(target_file)
