from yamlPipelineFactory import QueueNode
from yamlPipelineFactory.csv import CsvReader, CsvWriter
from yamlPipelineFactory.exceptions import StopProcess, StopBucket
import pytest
import asyncio

csv_input = """1,2,3
4,5,6
7,8,9
"""


@pytest.mark.asyncio
async def test_csv_reader(mocker):
    mocker.patch("builtins.open", mocker.mock_open(read_data=csv_input))

    filename_queue = QueueNode()
    row_queue = QueueNode()

    csv_reader = CsvReader(filename_queue, row_queue)

    task = asyncio.create_task(csv_reader.run())

    await filename_queue.put("test-1.csv")
    await filename_queue.put("test-2.csv")
    await filename_queue.put(StopProcess)
    for expected_row in [["1", "2", "3"],
                         ["4", "5", "6"],
                         ["7", "8", "9"],
                         StopBucket,
                         ["1", "2", "3"],
                         ["4", "5", "6"],
                         ["7", "8", "9"],
                         StopBucket,
                         StopProcess]:
        assert await row_queue.get() == expected_row
        row_queue.task_done()

    await task


@pytest.mark.asyncio
async def test_csv_writer():
    pass
