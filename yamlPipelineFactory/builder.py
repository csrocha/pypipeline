from .yaml import load, Loader


class Builder:
    def __init__(self, input_stream):
        self._pipeline = load(input_stream, Loader=Loader)

    def run(self):
        self._pipeline.run()
