from assemply import QueueNode
from assemply.atools import azip
from assemply.exceptions import StopBucket
import pytest


@pytest.mark.asyncio
async def test_azip_stop():

    qa = QueueNode()
    qb = QueueNode()
    qc = QueueNode()

    await qa.put(1)
    await qb.put(2)
    await qc.put(3)
    await qa.put(StopBucket(1))
    await qb.put(StopBucket(1))
    await qc.put(StopBucket(1))
    await qa.put(4)
    await qb.put(5)
    await qc.put(6)

    r = [(a, b, c) async for a, b, c in azip(qa, qb, qc)]

    assert r == [(1, 2, 3)]
