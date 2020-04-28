from yaml import YAMLObject, Loader


class Pipeline(YAMLObject):
    yaml_loader = Loader
    yaml_tag = u"!Pipeline"

    @classmethod
    def from_yaml(cls, loader, node):
        data = loader.construct_sequence(node)
        return cls(tasks=data)

    def __init__(self, name=None, tasks=None):
        self._name = name
        self._tasks = tasks

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self._name}, tasks={self._tasks})"

    def run(self):
        self._tasks[-1].run()