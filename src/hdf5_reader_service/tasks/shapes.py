import h5py

from hdf5_reader_service.model import DataTree, ShapeMetadata
from hdf5_reader_service.utils import h5_tree_map


def fetch_shapes(path: str, subpath: str, swmr: bool) -> DataTree[ShapeMetadata]:
    path = "/" + path

    def get_shape(name: str, obj: h5py.HLObject) -> ShapeMetadata:
        if hasattr(obj, "shape") and obj.shape != ():  # type: ignore
            return ShapeMetadata(shape=obj.shape)  # type: ignore
        else:
            return ShapeMetadata()

    with h5py.File(path, "r", swmr=swmr, libver="latest") as f:
        return h5_tree_map(get_shape, f[subpath])
