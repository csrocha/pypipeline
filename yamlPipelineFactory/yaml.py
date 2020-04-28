from yaml import load, dump, YAMLObject, add_constructor

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

__all__ = [load, dump, YAMLObject, add_constructor]
