import asyncio
from .yaml import load, Loader


class Builder:
    """
    Host class to build a pipeline from an YAML blueprint.
    """
    def __init__(self, input_stream):
        """
        Construtor of a YAML builder.

        :param input_stream: YAML blueprint.
        :type input_stream: str, io.Stream
        """
        self._pipeline = load(input_stream, Loader=Loader)

    def run(self):
        """
        Run the pipeline in asynchronous mode. Wait to end full pipeline.

        :return:
        :rtype:
        """
        asyncio.run(self._pipeline.run())

