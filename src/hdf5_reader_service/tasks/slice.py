import base64
from io import BytesIO
from typing import Optional

import h5py
import numpy as np


def fetch_slice(
    path: str, subpath: str, slice_info: Optional[str], swmr: bool
) -> bytes:
    path = "/" + path

    if slice_info is not None:
        # Create slice objects from strings, e.g.
        # convert "1:2:1,3:4:1" to tuple(slice(1, 2, 1), slice(3, 4, 1))
        slices = tuple(
            map(lambda t: slice(*map(int, t.split(":"))), slice_info.split(","))
        )

    with h5py.File(path, "r", swmr=swmr, libver="latest") as f:
        if subpath in f:
            dataset = f[subpath]
            if isinstance(dataset, h5py.Dataset):
                buffer = BytesIO()
                np.save(buffer, dataset[slices], allow_pickle=False)

                return base64.b64encode(buffer.getvalue())
            else:
                raise KeyError(
                    f"Expected {subpath} to be a dataset, \
                        it is acually a {type(dataset)}"
                )
        else:
            raise KeyError(f"{path} does not contain {subpath}")
