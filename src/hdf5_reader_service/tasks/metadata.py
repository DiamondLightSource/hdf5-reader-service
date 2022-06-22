import h5py

from hdf5_reader_service.model import (
    ByteOrder,
    DatasetMacroStructure,
    DatasetMicroStructure,
    DatasetStructure,
    MetadataNode,
)


def fetch_metadata(path: str, subpath: str, swmr: bool) -> MetadataNode:

    path = "/" + path

    with h5py.File(path, "r", swmr=swmr, libver="latest") as f:
        if subpath:
            meta = metadata(f[subpath])
        else:
            meta = metadata(f["/"])
        return meta


def metadata(node: h5py.HLObject) -> MetadataNode:
    name = node.name
    attributes = dict(node.attrs)

    data = MetadataNode(name=name, attributes=attributes)

    if isinstance(node, h5py.Dataset):
        shape = node.shape
        chunks = node.chunks
        itemsize = node.dtype.itemsize
        kind = node.dtype.kind
        byte_order = ByteOrder.of_hdf5_dataset(node)

        structure = DatasetStructure(
            macro=DatasetMacroStructure(chunks=chunks, shape=shape),
            micro=DatasetMicroStructure(
                itemsize=itemsize, kind=kind, byte_order=byte_order
            ),
        )
        data.structure = structure

    return data
