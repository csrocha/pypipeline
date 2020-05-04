from yamlPipelineFactory import QueueNode
from yamlPipelineFactory.atools import azip
from yamlPipelineFactory.exceptions import StopBucket
import pytest


@pytest.mark.asyncio
async def test_azip_stop():

    qa = QueueNode()
    qb = QueueNode()
    qc = QueueNode()

    await qa.put(1)
    await qb.put(2)
    await qc.put(3)
    await qa.put(StopBucket)
    await qb.put(StopBucket)
    await qc.put(StopBucket)
    await qa.put(4)
    await qb.put(5)
    await qc.put(6)

    r = [(a, b, c) async for a, b, c in azip(qa, qb, qc)]

    assert r == [(1, 2, 3)]
