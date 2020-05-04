from yamlPipelineFactory import Builder, node_sub
import os


@node_sub("!Test_process", inputs=("source",), outputs=("target",))
def test(source):
    return [int(source[0])**int(source[1])]


def _test_nodesub(event_loop):
    """
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
    - !Test_process
        source: *TestInput
        target: !Queue &MyNewProcessorOutput
    - !CsvWriter
        source: *MyNewProcessorOutput
        filename: test_output.csv
"""
    builder = Builder(yaml_string, event_loop)
    builder.run()

    with open("test_output.csv") as outfile:
        data = outfile.read()
        assert int(data) == 1

    os.remove("test_input.csv")
    os.remove("test_output.csv")
