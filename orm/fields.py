import typing
from pydantic import BaseModel
from pydantic.dataclasses import dataclass


@dataclass
class CustomField:
    # class CustomField(BaseModel):
    id: typing.Any = None


def Foreign(klass):
    return typing.Union[klass, CustomField]


def is_related_field(_type):
    class_type = hasattr(_type, "__args__")
    if class_type:
        return CustomField in _type.__args__
    return False


def is_json_field(_type):
    if hasattr(_type, "__args__"):
        return set(JSON_TYPE.__args__).issubset(_type.__args__)


JSON_TYPE = typing.Union[dict, list, tuple, set]


def JSON():
    return JSON_TYPE

