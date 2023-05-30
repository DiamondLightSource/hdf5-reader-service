from fastapi import FastAPI

from .api import router

# Setup the app
app = FastAPI()
app.include_router(router)
