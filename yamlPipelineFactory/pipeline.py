import asyncio
from .processor import processor


@processor('!Pipeline')
class Pipeline:
    """
    Pipeline class. Store all task which must be executed in parallel.
    """
    def __init__(self, name=None, tasks=None):
        self._name = name
        self._tasks = tasks

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self._name}, tasks={self._tasks})"

    async def run(self):
        async_tasks = [asyncio.create_task(task.run()) for task in self._tasks]

        await asyncio.gather(*async_tasks)
