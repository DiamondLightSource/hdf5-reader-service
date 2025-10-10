import json
import os
from dataclasses import asdict

from fastapi import APIRouter, Request
from starlette.responses import JSONResponse, StreamingResponse

from hdf5_reader_service.model import (
    DataTree,
    MetadataNode,
    NodeChildren,
    ShapeMetadata,
)
from hdf5_reader_service.utils import NumpySafeJSONResponse

from .fork import fork_and_do
from .tasks import (
    fetch_children,
    fetch_map,
    fetch_metadata,
    fetch_shapes,
    fetch_slice,
    fetch_tree,
)

SWMR_DEFAULT = bool(int(os.getenv("HDF5_SWMR_DEFAULT", "1")))

router = APIRouter()


@router.get("/info/", response_model=MetadataNode)
def get_info(path: str, subpath: str = "/") -> JSONResponse:
    """Function that tells flask to output the info of the HDF5 file node."""
    info = fork_and_do(fetch_metadata, args=(path, subpath, SWMR_DEFAULT))
    return NumpySafeJSONResponse(info)


@router.get("/search/", response_model=NodeChildren)
def get_children(path: str, subpath: str = "/") -> JSONResponse:
    """Function that tells flask to output the subnodes of the HDF5 file node."""
    nodes = fork_and_do(fetch_children, args=(path, subpath, SWMR_DEFAULT))
    return NumpySafeJSONResponse(nodes)


@router.get("/shapes/", response_model=DataTree[ShapeMetadata])
def get_shapes(path: str, subpath: str = "/") -> JSONResponse:
    """Function that tells flask to get the shapes of the HDF5 datasets."""
    shapes = fork_and_do(fetch_shapes, args=(path, subpath, SWMR_DEFAULT))
    return NumpySafeJSONResponse(shapes)


@router.get("/slice/")
def get_slice(
    path: str, subpath: str = "/", slice_info: str | None = None
) -> JSONResponse:
    """Function that tells flask to output the metadata of the HDF5 file node.
    The slice_info parameter should take the form
    start:stop:steps,start:stop:steps,...
    """
    data_slice = fork_and_do(
        fetch_slice, args=(path, subpath, slice_info, SWMR_DEFAULT)
    )
    return NumpySafeJSONResponse(data_slice)


@router.get("/tree/", response_model=DataTree[MetadataNode])
def get_tree(path: str, subpath: str = "/") -> JSONResponse:
    """Function that tells flask to render the tree of the HDF5 file."""
    tree = fork_and_do(fetch_tree, args=(path, subpath, SWMR_DEFAULT))
    return NumpySafeJSONResponse(tree)


@router.get("/map")
def get_map(filepath, datapath) -> JSONResponse:
    r = fork_and_do(fetch_map, args=(filepath, datapath, SWMR_DEFAULT))
    return NumpySafeJSONResponse(r)


@router.get("/events")
async def events(request: Request):
    tracker = request.app.state.tracker

    async def event_stream():
        # each client iterates over the async generator from scan tracker
        async for msg in tracker.listen():
            # msg is already a dict snapshot
            yield f"data: {json.dumps(msg)}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.get("/scans/latest")
def get_latest(request: Request):
    tracker = request.app.state.tracker
    latest = tracker.get_latest()
    return JSONResponse(asdict(latest))
