import pytest
from assemply.exceptions import StopBucket


async def assert_stop_bucket(queue, level):
    __tracebackhide__ = True
    data = await queue.get()
    if not (isinstance(data, StopBucket) and data.level == level):
        pytest.fail(f"Data {data} is not stop bucket at level {level}")


async def raw_push(queue, content):
    for item in content:
        await queue.put(item)
