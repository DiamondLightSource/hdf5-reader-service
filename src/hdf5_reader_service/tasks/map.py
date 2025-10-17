import h5py
import numpy as np


def fetch_map(path: str, subpath: str, swmr: bool) -> dict[str, list[list[float]]]:
    path = "/" + path

    with h5py.File(path, "r", swmr=swmr, libver="latest") as f:
        shape = f["/entry/plan_metadata/shape"]
        flat = f[subpath]
        if isinstance(shape, h5py.Dataset) and isinstance(flat, h5py.Dataset):
            n = len(flat)
            n_y, n_x = shape

            filled_rows, remainder = divmod(n, n_x)
            if remainder > 0:
                filled_rows += 1

            data = np.full([n_y, n_x], np.nan)
            partial = np.array(flat)
            data.flat[:n] = partial

            return {"values": data.tolist()}
        else:
            raise KeyError
