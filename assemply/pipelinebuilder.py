import asyncio
from .yaml import load, Loader


class PipelineBuilder:
    """
    Host class to build a pipeline from an YAML blueprint.
    """
    def __init__(self, blueprint, event_loop=None):
        """
        Constructor of a YAML builder.

        :param blueprint: YAML blueprint.
        :type blueprint: str, io.Stream
        """
        self._blueprint = blueprint
        self._event_loop = event_loop if event_loop else asyncio.get_event_loop()
        self._task = None

    def build(self):
        return load(self._blueprint, Loader=Loader)
