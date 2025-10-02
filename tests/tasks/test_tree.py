from collections.abc import Mapping
from pathlib import Path

import pytest

from hdf5_reader_service.model import (
    ByteOrder,
    DatasetMacroStructure,
    DatasetMicroStructure,
    DatasetStructure,
    DataTree,
    InvalidNode,
    InvalidNodeReason,
    MetadataNode,
    ValidNode,
)
from hdf5_reader_service.tasks import fetch_tree

TEST_CASES: Mapping[str, DataTree[MetadataNode]] = {
    "/entry/sample/name": DataTree(
        name="name",
        valid=True,
        node=ValidNode(
            contents=MetadataNode(
                name="/entry/sample/name",
                attributes={},
                structure=DatasetStructure(
                    macro=DatasetMacroStructure(shape=(), chunks=None),
                    micro=DatasetMicroStructure(
                        itemsize=8, kind="O", byte_order=ByteOrder.NOT_APPLICABLE
                    ),
                ),
            ),
            subnodes=[],
        ),
    ),
    "/entry/sample": DataTree(
        name="sample",
        valid=True,
        node=ValidNode(
            contents=MetadataNode(
                name="/entry/sample",
                attributes={"NX_class": "NXsample"},
                structure=None,
            ),
            subnodes=[
                DataTree(
                    name="description",
                    valid=True,
                    node=ValidNode(
                        contents=MetadataNode(
                            name="/entry/sample/description",
                            attributes={},
                            structure=DatasetStructure(
                                macro=DatasetMacroStructure(shape=(), chunks=None),
                                micro=DatasetMicroStructure(
                                    itemsize=8,
                                    kind="O",
                                    byte_order=ByteOrder.NOT_APPLICABLE,
                                ),
                            ),
                        ),
                        subnodes=[],
                    ),
                ),
                DataTree(
                    name="name",
                    valid=True,
                    node=ValidNode(
                        contents=MetadataNode(
                            name="/entry/sample/name",
                            attributes={},
                            structure=DatasetStructure(
                                macro=DatasetMacroStructure(shape=(), chunks=None),
                                micro=DatasetMicroStructure(
                                    itemsize=8,
                                    kind="O",
                                    byte_order=ByteOrder.NOT_APPLICABLE,
                                ),
                            ),
                        ),
                        subnodes=[],
                    ),
                ),
            ],
        ),
    ),
    "/entry/diamond_scan": DataTree(
        name="diamond_scan",
        valid=True,
        node=ValidNode(
            contents=MetadataNode(
                name="/entry/diamond_scan",
                attributes={"NX_class": "NXcollection"},
                structure=None,
            ),
            subnodes=[
                DataTree(
                    name="duration",
                    valid=True,
                    node=ValidNode(
                        contents=MetadataNode(
                            name="/entry/diamond_scan/duration",
                            attributes={
                                "target": "/entry/diamond_scan/duration",
                                "units": "ms",
                            },
                            structure=DatasetStructure(
                                macro=DatasetMacroStructure(shape=(), chunks=None),
                                micro=DatasetMicroStructure(
                                    itemsize=8, kind="i", byte_order=ByteOrder.NATIVE
                                ),
                            ),
                        ),
                        subnodes=[],
                    ),
                ),
                DataTree(
                    name="end_time",
                    valid=True,
                    node=ValidNode(
                        contents=MetadataNode(
                            name="/entry/diamond_scan/end_time",
                            attributes={"target": "/entry/diamond_scan/end_time"},
                            structure=DatasetStructure(
                                macro=DatasetMacroStructure(shape=(), chunks=None),
                                micro=DatasetMicroStructure(
                                    itemsize=1024,
                                    kind="S",
                                    byte_order=ByteOrder.NOT_APPLICABLE,
                                ),
                            ),
                        ),
                        subnodes=[],
                    ),
                ),
                DataTree(
                    name="keys",
                    valid=True,
                    node=ValidNode(
                        contents=MetadataNode(
                            name="/entry/diamond_scan/keys",
                            attributes={"NX_class": "NXcollection"},
                            structure=None,
                        ),
                        subnodes=[
                            DataTree(
                                name="uid",
                                valid=True,
                                node=ValidNode(
                                    contents=MetadataNode(
                                        name="/entry/uid",
                                        attributes={},
                                        structure=DatasetStructure(
                                            macro=DatasetMacroStructure(
                                                shape=(20, 20, 1, 1),
                                                chunks=(1, 1, 1, 1),
                                            ),
                                            micro=DatasetMicroStructure(
                                                itemsize=4,
                                                kind="i",
                                                byte_order=ByteOrder.NATIVE,
                                            ),
                                        ),
                                    ),
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
                    node=ValidNode(
                        contents=MetadataNode(
                            name="/entry/diamond_scan/scan_dead_time",
                            attributes={"units": "ms"},
                            structure=DatasetStructure(
                                macro=DatasetMacroStructure(shape=(), chunks=None),
                                micro=DatasetMicroStructure(
                                    itemsize=8, kind="i", byte_order=ByteOrder.NATIVE
                                ),
                            ),
                        ),
                        subnodes=[],
                    ),
                ),
                DataTree(
                    name="scan_dead_time_percent",
                    valid=True,
                    node=ValidNode(
                        contents=MetadataNode(
                            name="/entry/diamond_scan/scan_dead_time_percent",
                            attributes={},
                            structure=DatasetStructure(
                                macro=DatasetMacroStructure(shape=(), chunks=None),
                                micro=DatasetMicroStructure(
                                    itemsize=4,
                                    kind="f",
                                    byte_order=ByteOrder.LITTLE_ENDIAN,
                                ),
                            ),
                        ),
                        subnodes=[],
                    ),
                ),
                DataTree(
                    name="scan_estimated_duration",
                    valid=True,
                    node=ValidNode(
                        contents=MetadataNode(
                            name="/entry/diamond_scan/scan_estimated_duration",
                            attributes={"units": "ms"},
                            structure=DatasetStructure(
                                macro=DatasetMacroStructure(shape=(), chunks=None),
                                micro=DatasetMicroStructure(
                                    itemsize=8, kind="i", byte_order=ByteOrder.NATIVE
                                ),
                            ),
                        ),
                        subnodes=[],
                    ),
                ),
                DataTree(
                    name="scan_finished",
                    valid=True,
                    node=ValidNode(
                        contents=MetadataNode(
                            name="/entry/diamond_scan/scan_finished",
                            attributes={},
                            structure=DatasetStructure(
                                macro=DatasetMacroStructure(shape=(1,), chunks=(8192,)),
                                micro=DatasetMicroStructure(
                                    itemsize=4, kind="i", byte_order=ByteOrder.NATIVE
                                ),
                            ),
                        ),
                        subnodes=[],
                    ),
                ),
                DataTree(
                    name="scan_models",
                    valid=True,
                    node=ValidNode(
                        contents=MetadataNode(
                            name="/entry/diamond_scan/scan_models",
                            attributes={},
                            structure=DatasetStructure(
                                macro=DatasetMacroStructure(shape=(), chunks=None),
                                micro=DatasetMicroStructure(
                                    itemsize=8,
                                    kind="O",
                                    byte_order=ByteOrder.NOT_APPLICABLE,
                                ),
                            ),
                        ),
                        subnodes=[],
                    ),
                ),
                DataTree(
                    name="scan_rank",
                    valid=True,
                    node=ValidNode(
                        contents=MetadataNode(
                            name="/entry/diamond_scan/scan_rank",
                            attributes={},
                            structure=DatasetStructure(
                                macro=DatasetMacroStructure(shape=(), chunks=None),
                                micro=DatasetMicroStructure(
                                    itemsize=4, kind="i", byte_order=ByteOrder.NATIVE
                                ),
                            ),
                        ),
                        subnodes=[],
                    ),
                ),
                DataTree(
                    name="scan_request",
                    valid=True,
                    node=ValidNode(
                        contents=MetadataNode(
                            name="/entry/diamond_scan/scan_request",
                            attributes={},
                            structure=DatasetStructure(
                                macro=DatasetMacroStructure(shape=(), chunks=None),
                                micro=DatasetMicroStructure(
                                    itemsize=8,
                                    kind="O",
                                    byte_order=ByteOrder.NOT_APPLICABLE,
                                ),
                            ),
                        ),
                        subnodes=[],
                    ),
                ),
                DataTree(
                    name="scan_shape",
                    valid=True,
                    node=ValidNode(
                        contents=MetadataNode(
                            name="/entry/diamond_scan/scan_shape",
                            attributes={"target": "/entry/diamond_scan/scan_shape"},
                            structure=DatasetStructure(
                                macro=DatasetMacroStructure(shape=(2,), chunks=None),
                                micro=DatasetMicroStructure(
                                    itemsize=4, kind="i", byte_order=ByteOrder.NATIVE
                                ),
                            ),
                        ),
                        subnodes=[],
                    ),
                ),
                DataTree(
                    name="start_time",
                    valid=True,
                    node=ValidNode(
                        contents=MetadataNode(
                            name="/entry/diamond_scan/start_time",
                            attributes={"target": "/entry/diamond_scan/start_time"},
                            structure=DatasetStructure(
                                macro=DatasetMacroStructure(shape=(), chunks=None),
                                micro=DatasetMicroStructure(
                                    itemsize=8,
                                    kind="O",
                                    byte_order=ByteOrder.NOT_APPLICABLE,
                                ),
                            ),
                        ),
                        subnodes=[],
                    ),
                ),
            ],
        ),
    ),
}


@pytest.mark.parametrize("subpath,expected", TEST_CASES.items())
def test_fetch_tree(
    test_data_path: Path, subpath: str, expected: DataTree[MetadataNode]
) -> None:
    tree = fetch_tree(str(test_data_path), subpath, True)
    assert expected == tree
