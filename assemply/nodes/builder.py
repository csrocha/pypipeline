from assemply.node import node_class
from assemply.pipelinebuilder import PipelineBuilder


@node_class('!Builder')
class Builder:
    """
    Builder node descriptor.

    Source: Queue of blueprints in Yaml format.
    Target: Queue of pipelines ready to start.
    """
    def __init__(self, source=None, target=None):
        """
        Constructor of the StaticPusher.

        :param source:  Blueprint in yaml to create a pipeline.
        :type source: Queue of Str
        :param target: Target queue to new pipelines.
        :type target: Queue of Pipeline
        """
        self._source = source
        self._target = target

    def __repr__(self):
        """
        Representation of the StaticPusher node.

        :return: Formatted string.
        :rtype: str
        """
        return f"{self.__class__.__name__}(source={self._source})"

    async def run(self):
        """
        Main loop to takes blueprints from source and push pipelines to the target queue.

        :return: None
        :rtype: None
        """
        async with self._target as target:
            async for blueprint in self._source:
                pipeline = PipelineBuilder(blueprint)
                target.put(pipeline)
