import multiprocessing as mp
import os
from typing import Any, Mapping

import h5py
from fastapi import APIRouter

from hdf5_reader_service.utils import NumpySafeJSONResponse

router = APIRouter()

SWMR_DEFAULT = bool(int(os.getenv("HDF5_SWMR_DEFAULT", "1")))


# Setup blueprint route
@router.get("/info/{path:path}")
def get_info(path: str, subpath: str = "/"):
    """Function that tells flask to output the info of the HDF5 file node.

    Returns:
        template: A rendered Jinja2 HTML template
    """
    p = mp.Process(target=fetch_info, args=(path, subpath))
    p.start()
    p.join()


def fetch_info(path, subpath):
    path = "/" + path

    with h5py.File(path, "r", swmr=SWMR_DEFAULT, libver="latest") as file:
        if subpath:
            meta = NumpySafeJSONResponse(metadata(file[subpath]))
        else:
            meta = NumpySafeJSONResponse(metadata(file["/"]))
        return meta


def metadata(node: h5py.HLObject) -> Mapping[str, Any]:
    metadata = dict(node.attrs)

    data = {"data": {"attributes": {"metadata": metadata}}}

    if isinstance(node, h5py.Dataset):
        shape = node.maxshape
        chunks = node.chunks
        itemsize = node.dtype.itemsize
        kind = node.dtype.kind
        # endianness = dtype.endiannness if dtype.endiannness else "not_applicable"

        macro = {"chunks": chunks, "shape": shape}
        micro = {"itemsize": itemsize, "kind": kind}
        structure = {"macro": macro, "micro": micro}

        data["data"]["attributes"]["structure"] = structure

    return data
