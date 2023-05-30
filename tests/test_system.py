from pathlib import Path

import numpy as np
import pytest
from fastapi.testclient import TestClient

from hdf5_reader_service.app import app
from hdf5_reader_service.model import (
    DataTree,
    MetadataNode,
    NodeChildren,
    ShapeMetadata,
)
from tests.tasks.test_metadata import TEST_CASES as METADATA_TEST_CASES
from tests.tasks.test_search import TEST_CASES as SEARCH_TEST_CASES
from tests.tasks.test_shapes import TEST_CASES as SHAPE_TEST_CASES
from tests.tasks.test_slice import TEST_CASES as SLICE_TEST_CASES
from tests.tasks.test_tree import TEST_CASES as TREE_TEST_CASES


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_read_main(
    client: TestClient,
):
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {
        "INFO": "Please provide a path to the HDF5 file, e.g. '/file/<path>'."
    }


@pytest.mark.parametrize("subpath,shape", SHAPE_TEST_CASES.items())
def test_read_shapes(
    client: TestClient,
    test_data_path: Path,
    subpath: str,
    shape: DataTree[ShapeMetadata],
):
    response = client.get(
        "/shapes/", params={"path": str(test_data_path), "subpath": subpath}
    )
    assert response.status_code == 200
    actual_shape = DataTree[ShapeMetadata].parse_obj(response.json())
    assert actual_shape == shape


@pytest.mark.parametrize("subpath,tree", TREE_TEST_CASES.items())
def test_read_tree(
    client: TestClient, test_data_path: Path, subpath: str, tree: DataTree[MetadataNode]
):
    response = client.get(
        "/tree/", params={"path": str(test_data_path), "subpath": subpath}
    )
    assert response.status_code == 200
    actual_tree = DataTree[MetadataNode].parse_obj(response.json())
    assert actual_tree == tree


@pytest.mark.parametrize("subpath,metadata", METADATA_TEST_CASES.items())
def test_read_info(
    client: TestClient, test_data_path: Path, subpath: str, metadata: MetadataNode
):
    response = client.get(
        "/info/", params={"path": str(test_data_path), "subpath": subpath}
    )
    assert response.status_code == 200
    actual_metadata = MetadataNode.parse_obj(response.json())
    assert actual_metadata == metadata


@pytest.mark.parametrize("subpath,children", SEARCH_TEST_CASES.items())
def test_read_search(
    client: TestClient, test_data_path: Path, subpath: str, children: NodeChildren
):
    response = client.get(
        "/search/", params={"path": str(test_data_path), "subpath": subpath}
    )
    assert response.status_code == 200
    actual_children = NodeChildren.parse_obj(response.json())
    assert actual_children == children


@pytest.mark.parametrize("slice_info,expected_array", SLICE_TEST_CASES.items())
def test_read_slice(
    client: TestClient,
    test_data_path: Path,
    slice_info: str,
    expected_array: np.ndarray,
):
    response = client.get(
        "/slice/",
        params={
            "path": str(test_data_path),
            "subpath": "/entry/DIFFRACTION/data",
            "slice_info": slice_info,
        },
    )
    assert response.status_code == 200
    data_slice = np.array(response.json())
    np.testing.assert_array_equal(data_slice, expected_array)
