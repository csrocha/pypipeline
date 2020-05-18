import asyncio
import pytest
from assemply import node_sub, QueueNode
from assemply.exceptions import StopProcess
from .helpers import assert_stop_bucket


@pytest.mark.asyncio
async def test_node_sub_1i_1o():
    """ This test check subrutine node types with one input and one output queues """

    @node_sub("!Test_1i_1o", inputs=("source",), outputs=("target",))
    def test_1i_1o(source):
        return [int(source[0])**int(source[1])]

    source_queue = QueueNode()
    target_queue = QueueNode()

    test_node = test_1i_1o(source=source_queue, target=target_queue)

    task = asyncio.create_task(test_node.run())

    await source_queue.put(["1", "2"])
    await source_queue.put(["2", "1"])
    await source_queue.put(["2", "2"])
    await source_queue.put(StopProcess())

    assert await target_queue.get() == [1]
    target_queue.task_done()
    assert await target_queue.get() == [2]
    target_queue.task_done()
    assert await target_queue.get() == [4]
    target_queue.task_done()
    await assert_stop_bucket(target_queue, 0)

    await task


@pytest.mark.asyncio
async def test_node_sub_2i_1o():
    """ This test check subrutine node types with two inputs and one output queues """

    @node_sub("!Test_2i_1o", inputs=("left", "right"), outputs=("target",))
    def test_1i_1o(left, right):
        return [int(left)**int(right)]

    left_queue = QueueNode()
    right_queue = QueueNode()
    target_queue = QueueNode()

    test_node = test_1i_1o(left=left_queue, right=right_queue, target=target_queue)

    task = asyncio.create_task(test_node.run())

    await left_queue.put(1)
    await right_queue.put(2)
    await left_queue.put(2)
    await left_queue.put(2)
    await right_queue.put(1)
    await right_queue.put(2)
    await left_queue.put(StopProcess())
    await right_queue.put(1)  # Must not needed.

    assert await target_queue.get() == [1]
    assert await target_queue.get() == [2]
    assert await target_queue.get() == [4]

    await task
