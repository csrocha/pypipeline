import asyncio
from .processor import processor


@processor('!Pipeline')
class Pipeline:
    """
    Pipeline class. Store all task which must be executed in parallel.
    """
    def __init__(self, name=None, nodes=None):
        """
        Pipeline class constructor. Take an identification name and a list of nodes.

        :param name: Identification name.
        :type name: str
        :param nodes: List of nodes involved in the pipeline, without any order specification.
        :type nodes: [Task]
        """
        self._name = name
        self._nodes = nodes

    def __repr__(self):
        """
        Representation of a Pipeline

        :return: String with pipeline information.
        :rtype: str
        """
        return f"{self.__class__.__name__}(name={self._name}, tasks={self._nodes})"

    async def run(self):
        """
        Start nodes executions.

        :return: None
        :rtype: None

        TODO: - Exception management.
        """
        tasks = [asyncio.create_task(node.run()) for node in self._nodes]

        await asyncio.gather(*tasks)
