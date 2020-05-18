from assemply import QueueNode
from assemply.nodes.csv import CsvReader, CsvWriter
from assemply.exceptions import StopProcess, StopBucket
import pytest
import asyncio
from unittest.mock import call

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
    await filename_queue.put(StopProcess())

    for expected_row in [["1", "2", "3"],
                         ["4", "5", "6"],
                         ["7", "8", "9"],
                         StopBucket(1),
                         ["1", "2", "3"],
                         ["4", "5", "6"],
                         ["7", "8", "9"],
                         StopBucket(1),
                         StopProcess()]:
        assert await row_queue.get() == expected_row
        row_queue.task_done()

    await task


@pytest.mark.asyncio
async def test_csv_writer(mocker):
    m = mocker.patch("builtins.open", mocker.mock_open())

    filename_queue = QueueNode()
    row_queue = QueueNode()

    csv_writer = CsvWriter(filename_queue, row_queue)

    task = asyncio.create_task(csv_writer.run())

    await row_queue.put(["1", "2", "3"])
    await row_queue.put(["4", "5", "6"])
    await row_queue.put(["7", "8", "9"])
    await row_queue.put(StopBucket(1))
    await row_queue.put(["1", "2", "3" ])
    await row_queue.put(["4", "5", "6"])
    await row_queue.put(["7", "8", "9"])
    await row_queue.put(StopBucket(1))
    await row_queue.put(StopProcess())

    await filename_queue.put("test-1.csv")
    await filename_queue.put("test-2.csv")
    await filename_queue.put(StopProcess())

    await task

    assert m.mock_calls == [call('test-1.csv', 'w'),
                            call().__enter__(),
                            call().write('1,2,3\r\n'),
                            call().write('4,5,6\r\n'),
                            call().write('7,8,9\r\n'),
                            call().__exit__(None, None, None),
                            call('test-2.csv', 'w'),
                            call().__enter__(),
                            call().write('1,2,3\r\n'),
                            call().write('4,5,6\r\n'),
                            call().write('7,8,9\r\n'),
                            call().__exit__(None, None, None)]
