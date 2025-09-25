import sys
from collections.abc import Mapping
from pathlib import Path

import numpy as np
import pytest

from hdf5_reader_service.tasks import fetch_slice

TEST_CASES: Mapping[str, np.ndarray] = {
    "0:1:1,0:1:1,0:1:1,0:10:1": np.array(
        [[[[18, 19, 19, 20, 20, 21, 21, 22, 22, 23]]]], dtype=np.uint8
    ),
    "0:1:1,0:1:1,0:3:1,0:3:1": np.array(
        [[[[18, 19, 19], [19, 19, 20], [19, 20, 20]]]], dtype=np.uint8
    ),
    "0:1:1,0:2:1,0:3:1,0:3:1": np.array(
        [
            [
                [[18, 19, 19], [19, 19, 20], [19, 20, 20]],
                [[8, 8, 9], [8, 9, 9], [9, 9, 9]],
            ]
        ],
        dtype=np.uint8,
    ),
    "0:3:1,0:3:1,0:3:1,0:3:1": np.array(
        [
            [
                [[18, 19, 19], [19, 19, 20], [19, 20, 20]],
                [[8, 8, 9], [8, 9, 9], [9, 9, 9]],
                [[25, 25, 26], [26, 26, 27], [26, 27, 28]],
            ],
            [
                [[2, 2, 2], [2, 2, 2], [2, 2, 2]],
                [[1, 1, 1], [1, 1, 1], [1, 1, 1]],
                [[3, 3, 4], [4, 4, 4], [4, 4, 4]],
            ],
            [
                [[6, 6, 6], [6, 6, 6], [6, 6, 7]],
                [[7, 8, 8], [8, 8, 8], [8, 8, 8]],
                [[28, 29, 29], [29, 30, 30], [30, 31, 31]],
            ],
        ],
        dtype=np.uint8,
    ),
    "2:7:1,10:14:1,9:10:1,0:3:1": np.array(
        [
            [[[40, 41, 42]], [[40, 41, 42]], [[30, 31, 32]], [[32, 33, 34]]],
            [[[41, 42, 43]], [[41, 42, 43]], [[37, 38, 39]], [[23, 23, 24]]],
            [[[40, 41, 42]], [[41, 43, 44]], [[39, 40, 41]], [[34, 35, 36]]],
            [[[41, 42, 43]], [[42, 43, 44]], [[31, 32, 32]], [[35, 36, 37]]],
            [[[40, 41, 42]], [[41, 42, 44]], [[35, 36, 37]], [[23, 24, 24]]],
        ],
        dtype=np.uint8,
    ),
    "0:10:4,0:2:1,0:20:2,0:20:10": np.array(
        [
            [
                [
                    [18, 23],
                    [19, 25],
                    [21, 26],
                    [22, 28],
                    [24, 30],
                    [25, 32],
                    [26, 33],
                    [28, 35],
                    [29, 37],
                    [31, 39],
                ],
                [
                    [8, 10],
                    [9, 11],
                    [9, 12],
                    [10, 13],
                    [11, 13],
                    [11, 14],
                    [12, 15],
                    [12, 16],
                    [13, 17],
                    [14, 18],
                ],
            ],
            [
                [
                    [28, 36],
                    [30, 39],
                    [33, 41],
                    [35, 44],
                    [37, 47],
                    [39, 49],
                    [41, 52],
                    [43, 55],
                    [46, 58],
                    [48, 61],
                ],
                [
                    [29, 37],
                    [31, 40],
                    [33, 42],
                    [35, 45],
                    [38, 48],
                    [40, 51],
                    [42, 54],
                    [44, 56],
                    [47, 59],
                    [49, 62],
                ],
            ],
            [
                [
                    [28, 36],
                    [30, 38],
                    [32, 41],
                    [34, 44],
                    [36, 46],
                    [39, 49],
                    [41, 52],
                    [43, 55],
                    [45, 57],
                    [48, 60],
                ],
                [
                    [6, 8],
                    [7, 8],
                    [7, 9],
                    [8, 10],
                    [8, 10],
                    [9, 11],
                    [9, 12],
                    [10, 12],
                    [10, 13],
                    [11, 14],
                ],
            ],
        ],
        dtype=np.uint8,
    ),
}


@pytest.mark.parametrize("slice_info,expected", TEST_CASES.items())
def test_fetch_slice(
    test_data_path: Path,
    slice_info: str,
    expected: np.ndarray,
) -> None:
    np.set_printoptions(threshold=sys.maxsize)
    data_slice = fetch_slice(
        str(test_data_path), "/entry/DIFFRACTION/data", slice_info, True
    )
    print(data_slice.tolist())
    np.testing.assert_array_equal(data_slice, expected)


def test_fetch_slice_without_slice_info(test_data_path: Path) -> None:
    with pytest.raises(KeyError):
        fetch_slice(str(test_data_path), "/entry", None, True)


def test_subpath_not_h5py_dataset(test_data_path: Path) -> None:
    with pytest.raises(KeyError):
        fetch_slice(
            str(test_data_path), "/entry/sample", "0:1:1", False
        )  # sample is a Group, not a dataset


def test_fetch_slice_subpath_not_in_path(test_data_path: Path) -> None:
    with pytest.raises(KeyError):
        slice_info = "2:7:1,10:14:1,9:10:1,0:3:1"
        fetch_slice(str(test_data_path), "/something_else", slice_info, True)


def test_fetch_slice_of_broken_link(test_data_path: Path) -> None:
    with pytest.raises(KeyError):
        fetch_slice(str(test_data_path), "/entry/DIFFRACTION/simx", None, True)
