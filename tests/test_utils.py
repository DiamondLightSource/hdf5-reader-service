from collections.abc import Mapping
from pathlib import Path
from typing import Any

import h5py as h5
import pytest

from hdf5_reader_service.model import (
    DataTree,
    InvalidNode,
    InvalidNodeReason,
    ValidNode,
)
from hdf5_reader_service.utils import h5_tree_map

# Test trees

NO_RECURSION: tuple[str, DataTree[str]] = (
    "/entry/sample/name",
    DataTree(name="name", valid=True, node=ValidNode(contents="META", subnodes=[])),
)

ONE_LEVEL_RECURSION: tuple[str, DataTree[str]] = (
    "/entry/sample",
    DataTree(
        name="sample",
        valid=True,
        node=ValidNode(
            contents="META",
            subnodes=[
                DataTree(
                    name="description",
                    valid=True,
                    node=ValidNode(contents="META", subnodes=[]),
                ),
                DataTree(
                    name="name",
                    valid=True,
                    node=ValidNode(contents="META", subnodes=[]),
                ),
            ],
        ),
    ),
)


TWO_LEVEL_RECURSION: tuple[str, DataTree[str]] = (
    "/entry/diamond_scan",
    DataTree(
        name="diamond_scan",
        valid=True,
        node=ValidNode(
            contents="META",
            subnodes=[
                DataTree(
                    name="duration",
                    valid=True,
                    node=ValidNode(contents="META", subnodes=[]),
                ),
                DataTree(
                    name="end_time",
                    valid=True,
                    node=ValidNode(contents="META", subnodes=[]),
                ),
                DataTree(
                    name="keys",
                    valid=True,
                    node=ValidNode(
                        contents="META",
                        subnodes=[
                            DataTree(
                                name="uid",
                                valid=True,
                                node=ValidNode(contents="META", subnodes=[]),
                            ),
                            DataTree(
                                name="izero",
                                valid=False,
                                node=InvalidNode(reason=InvalidNodeReason.MISSING_LINK),
                            ),
                        ],
                    ),
                ),
                DataTree(
                    name="scan_dead_time",
                    valid=True,
                    node=ValidNode(contents="META", subnodes=[]),
                ),
                DataTree(
                    name="scan_dead_time_percent",
                    valid=True,
                    node=ValidNode(contents="META", subnodes=[]),
                ),
                DataTree(
                    name="scan_estimated_duration",
                    valid=True,
                    node=ValidNode(contents="META", subnodes=[]),
                ),
                DataTree(
                    name="scan_finished",
                    valid=True,
                    node=ValidNode(contents="META", subnodes=[]),
                ),
                DataTree(
                    name="scan_models",
                    valid=True,
                    node=ValidNode(contents="META", subnodes=[]),
                ),
                DataTree(
                    name="scan_rank",
                    valid=True,
                    node=ValidNode(contents="META", subnodes=[]),
                ),
                DataTree(
                    name="scan_request",
                    valid=True,
                    node=ValidNode(contents="META", subnodes=[]),
                ),
                DataTree(
                    name="scan_shape",
                    valid=True,
                    node=ValidNode(contents="META", subnodes=[]),
                ),
                DataTree(
                    name="start_time",
                    valid=True,
                    node=ValidNode(contents="META", subnodes=[]),
                ),
            ],
        ),
    ),
)


LINKED_DATA: tuple[str, DataTree[str]] = (
    "/entry/DIFFRACTION",
    DataTree(
        name="DIFFRACTION",
        valid=True,
        node=ValidNode(
            contents="META",
            subnodes=[
                DataTree(
                    name="data",
                    valid=True,
                    node=ValidNode(contents="META", subnodes=[]),
                ),
                DataTree(
                    name="simx",
                    valid=False,
                    node=InvalidNode(reason=InvalidNodeReason.MISSING_LINK),
                ),
                DataTree(
                    name="simy",
                    valid=False,
                    node=InvalidNode(reason=InvalidNodeReason.MISSING_LINK),
                ),
            ],
        ),
    ),
)


@pytest.mark.parametrize(
    "path,expected_tree",
    [NO_RECURSION, ONE_LEVEL_RECURSION, TWO_LEVEL_RECURSION, LINKED_DATA],
)
def test_h5_visit_map(
    test_data_path: Path, path: str, expected_tree: Mapping[str, Any]
) -> None:
    with h5.File(test_data_path) as f:
        tree = h5_tree_map(lambda name, obj: "META", f[path])

    from pprint import pprint

    pprint(tree)
    assert expected_tree == tree
