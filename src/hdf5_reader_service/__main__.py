import click

from . import __version__
from .app import app


@app.get("/")
def index():
    return {"INFO": "Please provide a path to the HDF5 file, e.g. '/file/<path>'."}


@click.group(invoke_without_command=True)
@click.option(
    "-h",
    "--host",
    type=str,
    help="host IP",
    default="0.0.0.0",
)
@click.option(
    "-p",
    "--port",
    type=int,
    help="host port",
    default="8000",
)
@click.version_option(version=__version__)
def main(host: str, port: int) -> None:
    import uvicorn

    uvicorn.run(app, host=host, port=port)
