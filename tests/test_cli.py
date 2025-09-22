import subprocess
import sys

from hdf5_reader_service import __version__


def test_cli_version():
    cmd = [sys.executable, "-m", "hdf5_reader_service", "--version"]
    assert subprocess.check_output(cmd).decode().strip() == __version__
