import pytest
from assemply.nodes.queuenode import QueueItemIterator, QueueBucketIterator
from assemply.exceptions import StopBucket
from .helpers import assert_stop_bucket, raw_push


@pytest.mark.asyncio
async def test_create_queuenode(queue):
    await raw_push(queue, [1, StopBucket(0)])
    assert 1 == await queue.get()
    await assert_stop_bucket(queue, 0)


@pytest.mark.asyncio
async def test_context_queuenode(queue):
    async with queue as q:
        await q.put(1)
    assert 1 == await queue.get()
    await assert_stop_bucket(queue, 0)


@pytest.mark.asyncio
async def test_contexts_queuenode(queue):
    async with queue:
        async with queue:
            await queue.put(1)
    assert 1 == await queue.get()
    await assert_stop_bucket(queue, 1)
    await assert_stop_bucket(queue, 0)


@pytest.mark.asyncio
async def test_contexts_entries_queuenode(queue):
    async with queue:
        async with queue:
            await queue.put(1)
        async with queue:
            await queue.put(1)

    assert 1 == await queue.get()
    await assert_stop_bucket(queue, 1)
    assert 1 == await queue.get()
    await assert_stop_bucket(queue, 1)
    await assert_stop_bucket(queue, 0)


@pytest.mark.asyncio
async def test_named_contexts_queuenode(queue):
    async with queue as P:
        async with P as B:
            await B.put(1)
    assert 1 == await queue.get()
    await assert_stop_bucket(queue, 1)
    await assert_stop_bucket(queue, 0)


@pytest.mark.asyncio
async def test_iterate_queuenode(queue):
    await raw_push(queue, [1, 2, StopBucket(0)])
    assert [data async for data in queue] == [1, 2]


@pytest.mark.asyncio
async def test_iterate_empty_queuenode(queue):
    await raw_push(queue, [StopBucket(0)])
    assert [data async for data in queue] == []


@pytest.mark.asyncio
async def test_iterate_bucket_queuenode_0_level(queue):
    await raw_push(queue, [0])
    b0 = queue.buckets(0)
    assert isinstance(b0, QueueItemIterator)
    b0_0 = await b0.__anext__()
    assert b0_0 == 0


@pytest.mark.asyncio
async def test_iterate_bucket_queuenode_1_level(queue):
    await raw_push(queue, [1])
    b1 = queue.buckets(1)
    assert isinstance(b1, QueueBucketIterator)
    b1_1 = await b1.__anext__()
    assert isinstance(b1_1, QueueItemIterator)
    b1_0 = await b1_1.__anext__()
    assert b1_0 == 1


@pytest.mark.asyncio
async def test_iterate_bucket_queuenode_2_levels(queue):
    await raw_push(queue, [2])
    b2 = queue.buckets(2)
    assert isinstance(b2, QueueBucketIterator)
    b2_2 = await b2.__anext__()
    assert isinstance(b2_2, QueueBucketIterator)
    b2_1 = await b2_2.__anext__()
    assert isinstance(b2_1, QueueItemIterator)
    b2_0 = await b2_1.__anext__()
    assert b2_0 == 2


@pytest.mark.asyncio
async def test_iterate_bucket_queuenode_0_levels_0_stop(queue):
    await raw_push(queue, [StopBucket(0)])
    b0 = queue.buckets(0)
    with pytest.raises(StopAsyncIteration):
        await b0.__anext__()


@pytest.mark.asyncio
async def test_iterate_bucket_queuenode_1_levels_0_stop(queue):
    await raw_push(queue, [StopBucket(0)])
    b1 = queue.buckets(1)
    with pytest.raises(StopAsyncIteration):
        await b1.__anext__()


@pytest.mark.asyncio
async def test_iterate_bucket_queuenode_1_levels_1_stop(queue):
    await raw_push(queue, [StopBucket(1)])
    b1 = queue.buckets(1)
    b0 = await b1.__anext__()
    with pytest.raises(StopAsyncIteration):
        await b0.__anext__()


@pytest.mark.asyncio
async def test_iterate_bucket_queuenode_3_levels_1_stop(queue):
    await raw_push(queue, [StopBucket(1)])
    b3 = queue.buckets(3)
    b2 = await b3.__anext__()
    with pytest.raises(StopAsyncIteration):
        await b2.__anext__()


@pytest.mark.asyncio
async def test_iterate_bucket_queuenode(queue):
    await raw_push(queue, [1, 2, StopBucket(1), StopBucket(0)])
    assert [data async for bucket in queue.buckets(1) async for data in bucket] == [1, 2]


@pytest.mark.asyncio
async def test_iterate_empty_bucket_queuenode(queue):
    await raw_push(queue, [StopBucket(1), StopBucket(0)])
    assert [data async for bucket in queue.buckets(1) async for data in bucket] == []


@pytest.mark.asyncio
async def test_iterate_2_buckets_queuenode(queue):
    await raw_push(queue, [1, 2, StopBucket(1), 3, 4, 5, StopBucket(1), StopBucket(0)])
    assert [[data async for data in bucket] async for bucket in queue.buckets(1)] ==\
           [[1, 2], [3, 4, 5]]


@pytest.mark.asyncio
async def test_iterate_3_buckets_level_0_queuenode(queue):
    await raw_push(queue, [1, 2, StopBucket(1), 3, 4, 5, StopBucket(1), StopBucket(1), StopBucket(0)])
    assert [[data async for data in bucket]
            async for bucket in queue.buckets(1)] == \
           [[1, 2], [3, 4, 5], []]


@pytest.mark.asyncio
async def test_iterate_3_buckets_level_1_queuenode(queue):
    await raw_push(queue, [1, 2, StopBucket(2), 3, 4, 5, StopBucket(2), StopBucket(1), StopBucket(0)])
    assert [[[data async for data in row] async for row in rows]
            async for rows in queue.buckets(2)] == \
           [[[1, 2], [3, 4, 5]]]


@pytest.mark.asyncio
async def test_iterate_3_buckets_2_3_1(queue):
    await raw_push(queue, [1, 2, StopBucket(2), 3, 4, 5, StopBucket(2), StopBucket(1), StopBucket(1), StopBucket(0)])
    assert [[[data async for data in row] async for row in rows]
            async for rows in queue.buckets(2)] == \
           [[[1, 2], [3, 4, 5]], []]


@pytest.mark.asyncio
async def test_iterate_3_buckets_1(queue):
    await raw_push(queue, [StopBucket(2), StopBucket(1), StopBucket(0)])
    assert [[[data async for data in row] async for row in rows]
            async for rows in queue.buckets(2)] == \
           [[[]]]
