from assemply import PipelineBuilder, node_class
import os
import pytest
import asyncio


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
async def test_my_processor_pipeline(tmp_path):
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


@node_class(u'!JoinProcessor')
class JoinProcessor:
    """
    This is a user level class to Node.
    """
    def __init__(self, left=None, right=None, joined=None, not_joined=None):
        """
        Create a new node with this sources and target.

        :param left: Input Queue
        :type left: Queue
        :param right: Input Queue
        :type right: Queue
        :param joined: Output Queue
        :type joined: Queue
        :param not_joined: Output Queue
        :type not_joined: Queue
        """
        self._left = left
        self._right = right
        self._joined = joined
        self._not_joined = not_joined

    async def run(self):
        """
        Main execution.

        :return: None
        :rtype: None
        """
        not_joined = {}
        rights = {}

        async def collect_right(q):
            async for right in self._right:
                rights[right[0]] = right[1:]
                if right[0] in not_joined:
                    await q.put(not_joined[right[0]] + right)
                    del not_joined[right[0]]

        async with self._joined as joined_q:
            cr = asyncio.create_task(collect_right(joined_q))

            async for left in self._left:
                if left[-1] in rights:
                    await joined_q.put(left[:-1] + rights[left[-1]])
                else:
                    not_joined[left[-1]] = left[:-1]

            await cr

        async with self._not_joined as not_joined_q:
            for k, nj in not_joined.items():
                await not_joined_q.put([k] + nj)


@pytest.mark.asyncio
async def test_join_pipeline(tmp_path):
    """
    Test to create and execute a pipeline using the node class Join

    Join user node do a left join between two queues, saving the joined rows and not joined rows in different queues.

    :return: --
    :rtype: --
    """
    left_source_file = tmp_path / "test_left_input.csv"
    right_source_file = tmp_path / "test_right_input.csv"
    join_target_file = tmp_path / "test_join_output.csv"
    not_join_target_file = tmp_path / "test_not_join_output.csv"

    with open(left_source_file, "w") as infile:
        for i, d in [(1, 'a'), (2, 'b'), (3, 'a'), (4, 'c'), (5, 'd')]:
            infile.write(f"{i},{d}\n")

    with open(right_source_file, "w") as infile:
        for i, d in [('a', 10), ('b', 20), ('c', 30)]:
            infile.write(f"{i},{d}\n")

    yaml_string = f"""
--- !Pipeline
name: Test pipeline
nodes:
    - !StaticPusher
        target: !Queue &LeftFileInput
        source:
        - {left_source_file}
    - !StaticPusher
        target: !Queue &RightFileInput
        source:
        - {right_source_file}
    - !StaticPusher
        target: !Queue &JoinFileOutput
        source:
        - {join_target_file}
    - !StaticPusher
        target: !Queue &NotJoinFileOutput
        source:
        - {not_join_target_file}
    - !CsvReader
        filename: *LeftFileInput
        target: !Queue &LeftTestInput
    - !CsvReader
        filename: *RightFileInput
        target: !Queue &RightTestInput
    - !JoinProcessor
        left: *LeftTestInput
        right: *RightTestInput
        joined: !Queue &JoinProcessorOutput
        not_joined: !Queue &NotJoinProcessorOutput
    - !CsvWriter
        source: *JoinProcessorOutput
        filename: *JoinFileOutput
    - !CsvWriter
        source: *NotJoinProcessorOutput
        filename: *NotJoinFileOutput
"""
    builder = PipelineBuilder(yaml_string)
    pipeline = builder.build()

    await pipeline.run()

    with open(join_target_file) as outfile:
        data = outfile.read()
        assert data == '3,a,10\n2,b,20\n4,c,30\n'

    with open(not_join_target_file) as outfile:
        data = outfile.read()
        assert data == 'd,5\n'

    os.remove(left_source_file)
    os.remove(right_source_file)
    os.remove(join_target_file)
    os.remove(not_join_target_file)
