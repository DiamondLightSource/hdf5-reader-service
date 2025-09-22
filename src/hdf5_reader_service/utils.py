import sys
from collections.abc import Callable, Mapping
from typing import Any, TypeVar

import h5py as h5
from pydantic import BaseModel
from starlette.responses import JSONResponse

from hdf5_reader_service.model import (
    DataTree,
    InvalidNode,
    InvalidNodeReason,
    ValidNode,
)


def safe_json_dump(content):
    """
    Try to use native orjson path; fall back to going through Python list.
    """
    import orjson

    def default(content):
        # No need to import numpy if it hasn't been used already.
        numpy = sys.modules.get("numpy", None)
        if numpy is not None:
            if isinstance(content, numpy.ndarray):
                # If we make it here, OPT_NUMPY_SERIALIZE failed because we have
                # hit some edge case. Give up on the numpy fast-path and convert
                # to Python list. If the items in this list aren't serializable
                # (e.g. bytes) we'll recurse on each item.
                return content.tolist()
            elif isinstance(content, (bytes, numpy.bytes_)):
                return content.decode("utf-8")
            elif isinstance(content, BaseModel):
                # Handle the pydantic model case
                return content.model_dump()
        raise TypeError

    # Not all numpy dtypes are supported by orjson.
    # Fall back to converting to a (possibly nested) Python list.
    return orjson.dumps(content, option=orjson.OPT_SERIALIZE_NUMPY, default=default)


class NumpySafeJSONResponse(JSONResponse):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def render(self, content: Any) -> bytes:
        return safe_json_dump(content)


#: Something that can be passed to json.dump
_Jsonable = Mapping[str, Any] | list[Any] | bool | int | float | str

#: Data type to map into tree
T = TypeVar("T")


def h5_tree_map(
    callback: Callable[[str, h5.HLObject], T], root: h5.HLObject
) -> DataTree[T]:
    name = root.name.split("/")[-1] if root.name else "root"
    block: DataTree[T] = DataTree(
        name=name,
        valid=True,
        node=ValidNode(contents=callback(name, root), subnodes=[]),
    )
    if isinstance(block.node, ValidNode):
        if hasattr(root, "items"):
            for k, v in root.items():  # type: ignore
                if v is not None:
                    block.node.subnodes.append(h5_tree_map(callback, v))
                else:
                    block.node.subnodes.append(
                        DataTree(
                            name=k,
                            valid=False,
                            node=InvalidNode(reason=InvalidNodeReason.MISSING_LINK),
                        )
                    )
    return block
