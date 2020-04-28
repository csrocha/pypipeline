from .yaml import add_constructor


def processor(yaml_tag):
    def inner(cls):

        def processor_constructor(loader, node):
            kwargs = loader.construct_mapping(node)
            return cls(**kwargs)

        add_constructor(yaml_tag, processor_constructor)

    return inner
