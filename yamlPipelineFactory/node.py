from .yaml import add_constructor
from .atools import azip


def node_class(yaml_tag):
    """
    Class decorator to publish a new node to YAML.
    More efficient and manageable node definition.

    :param yaml_tag: Tag name beginning with '!'.
    :type yaml_tag: str
    :return: Decorated class included in the loader constructor.
    :rtype: class

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


def node_sub(yaml_tag, inputs=None, outputs=None):
    """
    Subrutine decorator to publish a new node to YAML.
    More efficient for simple functions.

    :param yaml_tag: Tag name beginning with '!'.
    :type yaml_tag: str
    :param inputs: Input queue names for the node.
    :type inputs: [str]
    :param outputs: Output queue names for the node.
    :type outputs: [str]
    :return: Subrutine decorated class
    :rtype: class
    """

    def inner(f):
        class SubrutineNode:
            def __init__(self, **kwargs):
                try:
                    self._inputs = {k: kwargs[k] for k in inputs}
                    self._outputs = {k: kwargs[k] for k in outputs}
                except KeyError as ke:
                    raise AttributeError(f"Attribute not exists on {f}.")

                self._put = self._put_one if len(outputs) == 1 else self._put_many

            def __repr__(self):
                return f"{yaml_tag}()"

            async def _put_one(self, r):
                await self._outputs[outputs[0]].put(r)

            async def _put_many(self, r):
                for o, v in zip(outputs, r):
                    await o.put(self._outputs[o], v)

            async def _with_out(self, out_queue, *out_queues):
                async with self._outputs[out_queue]:
                    if out_queues:
                        await self._with_out(*out_queues)
                    else:
                        await self._in()

            async def _in(self, *out_queues):
                async for in_args in azip(*self._inputs.values()):
                    args = dict(zip(self._inputs.keys(), in_args))
                    await self._put(f(**args))

            async def run(self):
                await self._with_out(*outputs)

        def processor_constructor(loader, node):
            kwargs = loader.construct_mapping(node)
            return SubrutineNode(**kwargs)

        add_constructor(yaml_tag, processor_constructor)

        return SubrutineNode

    return inner
