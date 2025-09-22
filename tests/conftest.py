from os.path import abspath
from pathlib import Path
from posixpath import dirname

import pytest

_TEST_DIR_PATH = Path(dirname(abspath(__file__)))
_TEST_DATA_PATH = _TEST_DIR_PATH / "test-data/p45-104.nxs"


@pytest.fixture(scope="session")
def test_data_path() -> Path:
    return _TEST_DATA_PATH
