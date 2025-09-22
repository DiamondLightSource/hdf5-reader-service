from collections.abc import Mapping
from enum import Enum
from typing import Any, Generic, TypeVar

import h5py as h5
from pydantic import BaseModel


class DatasetMacroStructure(BaseModel):
    shape: tuple[int, ...]
    chunks: tuple[int, ...] | None = None


class ByteOrder(Enum):
    NATIVE = "NATIVE"
    LITTLE_ENDIAN = "LITTLE_ENDIAN"
    BIG_ENDIAN = "BIG_ENDIAN"
    NOT_APPLICABLE = "NOT_APPLICABLE"

    @classmethod
    def of_hdf5_dataset(cls, dataset: h5.Dataset) -> "ByteOrder":
        return {
            "=": cls.NATIVE,
            "<": cls.LITTLE_ENDIAN,
            ">": cls.BIG_ENDIAN,
            "|": cls.NOT_APPLICABLE,
        }[dataset.dtype.byteorder]


class DatasetMicroStructure(BaseModel):
    itemsize: int
    kind: str
    byte_order: ByteOrder = ByteOrder.NOT_APPLICABLE


class DatasetStructure(BaseModel):
    macro: DatasetMacroStructure
    micro: DatasetMicroStructure


class MetadataNode(BaseModel):
    name: str
    attributes: Mapping[str, Any]
    structure: DatasetStructure | None = None


class NodeChildren(BaseModel):
    nodes: list[str]


class ShapeMetadata(BaseModel):
    shape: tuple[int, ...] | None = None


T = TypeVar("T")


class ValidNode(BaseModel, Generic[T]):
    contents: T
    subnodes: list["DataTree"] = []


class InvalidNodeReason(Enum):
    MISSING_LINK = "MISSING_LINK"
    NOT_FOUND = "NOT_FOUND"


class InvalidNode(BaseModel):
    reason: InvalidNodeReason


class DataTree(BaseModel, Generic[T]):
    name: str
    valid: bool
    node: InvalidNode | ValidNode[T]


ValidNode.model_rebuild()
