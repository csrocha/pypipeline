import asyncio
from assemply.node import node_class


@node_class('!Pipeline')
class Pipeline:
    """
    Pipeline class. Store all task which must be executed in parallel.
    """
    def __init__(self, name=None, nodes=None, expose=None):
        """
        Pipeline class constructor. Take an identification name and a list of nodes.

        :param name: Identification name.
        :type name: str
        :param nodes: List of nodes involved in the pipeline, without any order specification.
        :type nodes: [Task]
        """
        self._name = name
        self._nodes = nodes
        self._expose = expose
        self._tasks = []

    def __repr__(self):
        """
        Pipeline string representation.

        :return: String with pipeline information.
        :rtype: str
        """
        return f"{self.__class__.__name__}(name={self._name}, tasks={self._nodes})"

    async def run(self):
        """
        Start nodes executions.

        :return: None
        :rtype: None
        """
        tasks = [asyncio.create_task(node.run()) for node in self._nodes]

        await asyncio.gather(*tasks)

    def name(self):
        return self._name

    def interface(self):
        return self._expose

    async def __aenter__(self):
        self._tasks.append(asyncio.create_task(self.run()))
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._tasks[-1]
        del self._tasks[-1]
        return

    def __getattr__(self, item):
        if item in self._expose:
            return self._expose[item]
        else:
            raise AttributeError
