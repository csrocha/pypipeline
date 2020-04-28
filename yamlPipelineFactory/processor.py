from .yaml import add_constructor


def processor(yaml_tag):
    """
    Class decorator to publish a process class to YAML.

    :param yaml_tag: Tag name beginning with '!'.
    :type yaml_tag: str
    :return: Decorated class included in the loader constructor.
    :rtype: Processor class.

    TODO: - Check if class interface has run function.
    """
    def inner(cls):

        def processor_constructor(loader, node):
            kwargs = loader.construct_mapping(node)
            return cls(**kwargs)

        add_constructor(yaml_tag, processor_constructor)

    return inner
