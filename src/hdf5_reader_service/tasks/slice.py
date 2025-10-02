import h5py
import numpy as np


def fetch_slice(
    path: str, subpath: str, slice_info: str | None, swmr: bool
) -> np.ndarray:
    path = "/" + path

    if slice_info is not None:
        # Create slice objects from strings, e.g.
        # convert "1:2:1,3:4:1" to tuple(slice(1, 2, 1), slice(3, 4, 1))
        slices = tuple(slice(*map(int, t.split(":"))) for t in slice_info.split(","))

        with h5py.File(path, "r", swmr=swmr, libver="latest") as f:
            if subpath in f:
                dataset = f[subpath]
                if isinstance(dataset, h5py.Dataset):
                    print(f"{subpath} IS a dataset!!!")
                    return dataset[slices]
                else:
                    raise KeyError(
                        f"Expected {subpath} to be a dataset, \
                            it is acually a {type(dataset)}"
                    )
            else:
                raise KeyError(f"{path} does not contain {subpath}")
    raise KeyError("Slice info not provided")
