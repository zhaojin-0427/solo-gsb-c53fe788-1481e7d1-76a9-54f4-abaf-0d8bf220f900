"""FastAPI application factory and uvicorn entrypoint."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .api import router
from .config import Settings
from .db import Database
from .errors import OCIError
from .service import RegistryService
from .storage import Storage

log = logging.getLogger("registry")


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    db = Database(settings.db_path)
    storage = Storage(settings.data_dir)
    service = RegistryService(settings, db, storage)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Resume/verify persistent state before serving traffic.
        service.reconcile_on_startup()
        yield

    app = FastAPI(title="Local Container Artifact Registry", version="1.0.0",
                  lifespan=lifespan)
    app.state.settings = settings
    app.state.db = db
    app.state.storage = storage
    app.state.service = service
    app.include_router(router)

    @app.exception_handler(OCIError)
    async def oci_error_handler(request: Request, exc: OCIError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content=exc.body(),
                            headers=exc.headers, media_type="application/json")

    @app.exception_handler(RequestValidationError)
    async def validation_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={"errors": [{"code": "BAD_REQUEST",
                                 "message": "Request validation failed.",
                                 "detail": exc.errors()}]},
        )

    return app


app = create_app()


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8080)
