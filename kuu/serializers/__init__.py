import importlib.util

from .base import Serializer
from .json import JSONSerializer
from .pickle import PickleSerializer

__all__ = ["JSONSerializer", "PickleSerializer", "Serializer"]
if importlib.util.find_spec("msgspec") is not None:
    from .msgpack import MsgpackSerializer

    __all__ = ["JSONSerializer", "PickleSerializer", "Serializer", "MsgpackSerializer"]
