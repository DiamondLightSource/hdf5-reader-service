import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from hdf5_reader_service.scan_tracker import ScanTracker

from .api import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_running_loop()
    tracker = ScanTracker(
        host="b01-1-rabbitmq-daq.diamond.ac.uk",
        port=61613,
        username="guest",
        password="guest",
        destination="public.worker.event",  # bare topic -> ScanTracker will prefix
        loop=loop,
    )
    app.state.tracker = tracker
    try:
        yield
    finally:
        tracker.disconnect()


app = FastAPI(lifespan=lifespan)
app.include_router(router)


@app.get("/")
def index():
    return {"INFO": "Please provide a path to the HDF5 file, e.g. '/file/<path>'."}
