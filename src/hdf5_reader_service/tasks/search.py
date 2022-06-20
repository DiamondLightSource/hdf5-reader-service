import multiprocessing as mp
from typing import Any, Mapping

import h5py


def fetch_children(path: str, subpath: str, swmr: bool, queue: mp.Queue) -> None:
    path = "/" + path

    with h5py.File(path, "r", swmr=swmr, libver="latest") as f:

        node = f[subpath]

        if not isinstance(node, h5py.Group):
            queue.put({"INFO": "Please provide a path to a dataset"})
        else:
            nodes = search(node)
            queue.put(nodes)


def search(node: h5py.Group) -> Mapping[str, Any]:
    subnodes = {"nodes": list(node.keys())}
    return subnodes
