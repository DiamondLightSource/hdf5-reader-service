import h5py
import numpy as np


def fetch_map(path: str, subpath: str, swmr: bool) -> np.ndarray:
    path = "/" + path

    with h5py.File(path, "r", swmr=swmr, libver="latest") as f:
        shape = f["/entry/plan_metadata/shape"]
        flat = f[subpath]
        if isinstance(shape, h5py.Dataset) and isinstance(flat, h5py.Dataset):
            return np.array(flat).reshape(shape)  # type: ignore
        else:
            raise KeyError
