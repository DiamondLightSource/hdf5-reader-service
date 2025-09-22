from collections.abc import Mapping
from pathlib import Path

import pytest

from hdf5_reader_service.model import (
    DataTree,
    InvalidNode,
    InvalidNodeReason,
    ShapeMetadata,
    ValidNode,
)
from hdf5_reader_service.tasks import fetch_shapes

TEST_CASES: Mapping[str, DataTree[ShapeMetadata]] = {
    "/entry/sample/name": DataTree(
        name="name", valid=True, node=ValidNode(contents=ShapeMetadata(), subnodes=[])
    ),
    "/entry/sample": DataTree(
        name="sample",
        valid=True,
        node=ValidNode(
            contents=ShapeMetadata(),
            subnodes=[
                DataTree(
                    name="description",
                    valid=True,
                    node=ValidNode(contents=ShapeMetadata(), subnodes=[]),
                ),
                DataTree(
                    name="name",
                    valid=True,
                    node=ValidNode(contents=ShapeMetadata(), subnodes=[]),
                ),
            ],
        ),
    ),
    "/entry/diamond_scan": DataTree(
        name="diamond_scan",
        valid=True,
        node=ValidNode(
            contents=ShapeMetadata(),
            subnodes=[
                DataTree(
                    name="duration",
                    valid=True,
                    node=ValidNode(contents=ShapeMetadata(), subnodes=[]),
                ),
                DataTree(
                    name="end_time",
                    valid=True,
                    node=ValidNode(contents=ShapeMetadata(), subnodes=[]),
                ),
                DataTree(
                    name="keys",
                    valid=True,
                    node=ValidNode(
                        contents=ShapeMetadata(),
                        subnodes=[
                            DataTree(
                                name="uid",
                                valid=True,
                                node=ValidNode(
                                    contents=ShapeMetadata(shape=(20, 20, 1, 1)),
                                    subnodes=[],
                                ),
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
                    node=ValidNode(contents=ShapeMetadata(), subnodes=[]),
                ),
                DataTree(
                    name="scan_dead_time_percent",
                    valid=True,
                    node=ValidNode(contents=ShapeMetadata(), subnodes=[]),
                ),
                DataTree(
                    name="scan_estimated_duration",
                    valid=True,
                    node=ValidNode(contents=ShapeMetadata(), subnodes=[]),
                ),
                DataTree(
                    name="scan_finished",
                    valid=True,
                    node=ValidNode(contents=ShapeMetadata(shape=(1,)), subnodes=[]),
                ),
                DataTree(
                    name="scan_models",
                    valid=True,
                    node=ValidNode(contents=ShapeMetadata(), subnodes=[]),
                ),
                DataTree(
                    name="scan_rank",
                    valid=True,
                    node=ValidNode(contents=ShapeMetadata(), subnodes=[]),
                ),
                DataTree(
                    name="scan_request",
                    valid=True,
                    node=ValidNode(contents=ShapeMetadata(), subnodes=[]),
                ),
                DataTree(
                    name="scan_shape",
                    valid=True,
                    node=ValidNode(contents=ShapeMetadata(shape=(2,)), subnodes=[]),
                ),
                DataTree(
                    name="start_time",
                    valid=True,
                    node=ValidNode(contents=ShapeMetadata(), subnodes=[]),
                ),
            ],
        ),
    ),
    "/entry/DIFFRACTION": DataTree(
        name="DIFFRACTION",
        valid=True,
        node=ValidNode(
            contents=ShapeMetadata(),
            subnodes=[
                DataTree(
                    name="data",
                    valid=True,
                    node=ValidNode(
                        contents=ShapeMetadata(shape=(20, 20, 120, 160)), subnodes=[]
                    ),
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
}


@pytest.mark.parametrize("subpath,expected", TEST_CASES.items())
def test_fetch_shapes(
    test_data_path: Path, subpath: str, expected: DataTree[ShapeMetadata]
) -> None:
    shapes = fetch_shapes(str(test_data_path), subpath, True)
    assert expected == shapes
