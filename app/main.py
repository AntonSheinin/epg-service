from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI, Request

from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.config import setup_logging
from app.database import init_db
from app.scheduler import epg_scheduler

from app.routers import main_router


setup_logging()
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events"""
    logger.info("Starting EPG Service...")
    await init_db()
    epg_scheduler.start()
    logger.info("EPG Service started successfully")

    yield

    logger.info("Shutting down EPG Service...")
    epg_scheduler.shutdown()
    logger.info("EPG Service stopped")


app = FastAPI(
    title="EPG Service",
    version="0.1.0",
    lifespan=lifespan
)

app.include_router(main_router)

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Log validation errors with details"""
    logger.error(f"Validation error for {request.method} {request.url.path}")
    logger.error(f"Validation details: {exc.errors()}")

    try:
        body = await request.body()
        logger.error(f"Request body: {body.decode('utf-8')}")

    except:
        logger.error("Could not read request body")

    # Create a properly serializable error response
    errors = []
    for error in exc.errors():
        error_dict = {
            "type": error.get("type"),
            "loc": error.get("loc"),
            "msg": error.get("msg"),
            "input": str(error.get("input", ""))[:100]
        }
        errors.append(error_dict)

    return JSONResponse(
        status_code=422,
        content={
            "detail": errors
        }
    )

