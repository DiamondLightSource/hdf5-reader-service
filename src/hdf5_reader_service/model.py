from enum import Enum
from typing import Any, Generic, List, Literal, Mapping, Optional, Tuple, TypeVar, Union

import h5py as h5
from pydantic import BaseModel, Field
from pydantic.generics import GenericModel


class DatasetMacroStructure(BaseModel):
    shape: Tuple[int, ...]
    chunks: Optional[Tuple[int, ...]] = None


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
    byte_order: ByteOrder


class DatasetStructure(BaseModel):
    macro: DatasetMacroStructure
    micro: DatasetMicroStructure


class MetadataNode(BaseModel):
    name: str
    attributes: Mapping[str, Any]
    structure: Optional[DatasetStructure] = None


class NodeChildren(BaseModel):
    nodes: List[str]


class ShapeMetadata(BaseModel):
    shape: Optional[Tuple[int, ...]] = None


T = TypeVar("T")


class ValidNode(GenericModel, Generic[T]):
    contents: T
    subnodes: List["DataTree"] = []
    type: Literal["ValidNodeMetadataNode_"] = "ValidNodeMetadataNode_"


class InvalidNodeReason(Enum):
    MISSING_LINK = "MISSING_LINK"
    NOT_FOUND = "NOT_FOUND"


class InvalidNode(BaseModel):
    reason: InvalidNodeReason
    type: Literal["InvalidNode"] = "InvalidNode"


class DataTree(GenericModel, Generic[T]):
    name: str
    valid: bool
    node: Union[InvalidNode, ValidNode[T]] = Field(discriminator="type")


ValidNode.update_forward_refs()
