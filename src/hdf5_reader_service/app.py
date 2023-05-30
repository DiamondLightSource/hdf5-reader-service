from fastapi import FastAPI

from .api import router

# Setup the app
app = FastAPI()
app.include_router(router)


@app.get("/")
def index():
    return {"INFO": "Please provide a path to the HDF5 file, e.g. '/file/<path>'."}
