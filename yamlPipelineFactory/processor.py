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
        """
        Inner processor function. Create the constructor for the class for the YAML parsing.

        :param cls: Class to add in the YAML loader.
        :type cls: Class
        :return: Added class
        :rtype: Class
        """

        def processor_constructor(loader, node):
            """
            Create a new instance of cls from YAML node information.

            :param loader: The loader process.
            :type loader: Loader
            :param node: The parameters node for the class.
            :type node: Node
            :return: Instance of the class with defined parameters in the node.
            :rtype: cls
            """
            kwargs = loader.construct_mapping(node)
            return cls(**kwargs)

        add_constructor(yaml_tag, processor_constructor)

        return cls

    return inner
