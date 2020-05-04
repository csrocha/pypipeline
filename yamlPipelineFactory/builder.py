import asyncio
from .yaml import load, Loader


class Builder:
    """
    Host class to build a pipeline from an YAML blueprint.
    """
    def __init__(self, input_stream, event_loop):
        """
        Construtor of a YAML builder.

        :param input_stream: YAML blueprint.
        :type input_stream: str, io.Stream
        """
        self._pipeline = load(input_stream, Loader=Loader)
        self._event_loop = event_loop if event_loop else asyncio.get_event_loop()

    def run(self):
        """
        Run the pipeline in asynchronous mode. Wait to end full pipeline.

        :return:
        :rtype:
        """
        self._event_loop.run_until_complete(self._pipeline.run())
