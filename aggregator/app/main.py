import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from .db import init_db, close_db
from .consumer import ConsumerWorker
from .routes import router
from .config import LOG_LEVEL

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    worker = ConsumerWorker()
    app.state.worker = worker
    await worker.start()
    yield
    await worker.stop()
    await close_db()

app = FastAPI(title="UTS Aggregator — Pub-Sub Log Aggregator", lifespan=lifespan)
app.include_router(router)
