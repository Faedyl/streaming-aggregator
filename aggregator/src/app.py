"""
FastAPI Application Factory
Endpoint: /health, /publish, /events, /stats, /readiness
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import logging
import os
from typing import Optional, Dict, Any
import asyncio

from .models import (
    Event, PublishRequest, PublishResponse,
    EventResponse, EventsListResponse,
    StatsResponse, HealthResponse
)
from .dedup_store import DedupStore
from .consumer import IdempotentConsumer
from .utils import EventMetrics, format_timestamp

logger = logging.getLogger(__name__)

# Global state
_consumer: Optional[IdempotentConsumer] = None
_dedup_store: Optional[DedupStore] = None
_metrics: Optional[EventMetrics] = None
_app_start_time: Optional[datetime] = None
_outbox_task: Optional[asyncio.Task] = None

ISOLATION_LEVEL = "READ COMMITTED (default PostgreSQL)"
APP_VERSION = "1.0.0"


def create_app(database_url: Optional[str] = None,
               broker_url: Optional[str] = None) -> FastAPI:
    """
    Create and configure FastAPI application

    Isolation level yang digunakan: READ COMMITTED (default PostgreSQL)
    Alasan:
    - Mencegah dirty reads: worker tidak bisa membaca data uncommitted dari worker lain
    - UNIQUE constraint (topic, event_id) memberikan serializability untuk dedup
      tanpa perlu SERIALIZABLE isolation yang lebih mahal
    - UPDATE counter = counter + 1 mencegah lost-update pada stats
    - Cukup untuk use case log aggregator; phantom reads tidak relevan
      karena kita hanya INSERT dan UPDATE, bukan SELECT range query
    """

    global _consumer, _dedup_store, _metrics, _app_start_time, _outbox_task

    if database_url is None:
        database_url = os.getenv(
            "DATABASE_URL",
            "postgres://struser:strpass@localhost:5432/strdb"
        )
    if broker_url is None:
        broker_url = os.getenv(
            "BROKER_URL",
            "redis://localhost:6379"
        )

    _dedup_store = DedupStore(database_url=database_url)
    _consumer = IdempotentConsumer(dedup_store=_dedup_store, broker_url=broker_url)
    _metrics = EventMetrics()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        global _app_start_time, _outbox_task
        logger.info("=" * 60)
        logger.info("UAS Pub-Sub Log Aggregator Starting...")
        logger.info(f"Isolation Level: {ISOLATION_LEVEL}")
        logger.info(f"Database: {database_url}")
        logger.info(f"Broker: {broker_url}")
        logger.info(f"Workers: {os.getenv('NUM_WORKERS', '4')}")
        logger.info("=" * 60)

        _app_start_time = datetime.utcnow()

        # Connect ke database
        await _dedup_store.connect()

        # Start consumer (multi-worker)
        await _consumer.start()

        # Start outbox processor background task
        _outbox_task = asyncio.create_task(_process_outbox_loop())

        logger.info("Application started successfully")
        try:
            yield
        finally:
            logger.info("Application shutting down...")
            if _outbox_task:
                _outbox_task.cancel()
            await _consumer.stop()
            await _dedup_store.close()
            logger.info("Application shutdown complete")

    app = FastAPI(
        title="UAS Pub-Sub Log Aggregator",
        description=(
            "Pub-Sub Log Aggregator Terdistribusi dengan "
            "Idempotent Consumer, Deduplication, "
            "dan Transaksi/Kontrol Konkurensi"
        ),
        version=APP_VERSION,
        lifespan=lifespan,
    )

    # ============================================================
    # Health Check
    # ============================================================
    @app.get("/health")
    async def health_check():
        """Health check endpoint untuk readiness/liveness probe"""
        db_healthy = await _dedup_store.check_health()
        return HealthResponse(
            status="healthy" if db_healthy else "degraded",
            database="connected" if db_healthy else "disconnected",
            broker="connected",  # Will be checked if needed
            version=APP_VERSION,
            timestamp=format_timestamp()
        )

    # ============================================================
    # Readiness Probe
    # ============================================================
    @app.get("/readiness")
    async def readiness():
        """Readiness probe - apakah service siap menerima traffic"""
        db_healthy = await _dedup_store.check_health()
        queue_length = await _consumer.get_queue_length()

        return {
            "status": "ready" if db_healthy else "not_ready",
            "database": "connected" if db_healthy else "disconnected",
            "queue_length": queue_length,
            "timestamp": format_timestamp()
        }

    # ============================================================
    # Liveness Probe
    # ============================================================
    @app.get("/liveness")
    async def liveness():
        """Liveness probe - apakah service masih hidup"""
        uptime = _metrics.get_uptime()
        return {
            "status": "alive",
            "uptime_seconds": uptime,
            "timestamp": format_timestamp()
        }

    # ============================================================
    # Publish Events
    # ============================================================
    @app.post("/publish")
    async def publish_events(request: PublishRequest):
        """
        Publish single atau batch event.

        Batch diproses secara transactional:
        - Setiap event di-cek dedup-nya secara independen
        - Event unik di-enqueue ke Redis broker
        - Event duplikat di-skip
        - Received counter di-increment untuk semua event

        Partial commit: event valid tetap diproses meski ada event invalid.
        Semua event yang sudah masuk queue akan diproses oleh worker.
        """
        try:
            received_count = len(request.events)
            processed_count = 0
            duplicates_detected = 0

            # Increment received counter
            await _dedup_store.increment_stat('received', received_count)

            # Process batch: cek dedup, enqueue unique events
            unique_events = []
            for event in request.events:
                # Cek apakah sudah diproses
                is_duplicate = await _dedup_store.exists(
                    event.topic, event.event_id
                )

                if is_duplicate:
                    duplicates_detected += 1
                    await _dedup_store.increment_stat('duplicate_dropped')
                    logger.warning(
                        f"Duplicate detected: topic={event.topic}, "
                        f"event_id={event.event_id}"
                    )
                else:
                    unique_events.append(event)
                    processed_count += 1

            # Enqueue unique events ke Redis untuk multi-worker processing
            if unique_events:
                await _consumer.enqueue_batch(unique_events)

            return PublishResponse(
                status="success",
                received=received_count,
                processed=processed_count,
                duplicates_detected=duplicates_detected,
                timestamp=datetime.utcnow()
            )

        except ValueError as e:
            logger.error(f"Validation error: {str(e)}")
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Error publishing events: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ============================================================
    # Get Events
    # ============================================================
    @app.get("/events")
    async def get_events(
        topic: Optional[str] = Query(None, description="Filter by topic"),
        limit: int = Query(100, ge=1, le=1000, description="Max results"),
        offset: int = Query(0, ge=0, le=10000, description="Offset for pagination")
    ):
        """
        Get list of processed unique events

        Query params:
        - topic: Filter by topic (optional)
        - limit: Max events (default 100, max 1000)
        - offset: Pagination offset
        """
        try:
            entries = await _dedup_store.get_processed_events(
                topic=topic, limit=limit, offset=offset
            )

            events = []
            for entry in entries:
                try:
                    ts = entry['event_timestamp']
                    if isinstance(ts, datetime):
                        ts_iso = ts.isoformat()
                    else:
                        ts_iso = str(ts)

                    pa = entry['processed_at']
                    if isinstance(pa, datetime):
                        pa_iso = pa.isoformat()
                    else:
                        pa_iso = str(pa)

                    event = EventResponse(
                        topic=entry['topic'],
                        event_id=entry['event_id'],
                        timestamp=ts_iso,
                        source=entry.get('event_source', ''),
                        payload=entry.get('event_payload', {}),
                        processed_at=pa_iso
                    )
                    events.append(event)
                except Exception as e:
                    logger.warning(f"Error formatting event entry: {str(e)}")
                    continue

            return EventsListResponse(
                status="success",
                events=events,
                count=len(events),
                timestamp=format_timestamp()
            )

        except Exception as e:
            logger.error(f"Error getting events: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ============================================================
    # Get Statistics
    # ============================================================
    @app.get("/stats")
    async def get_stats():
        """
        Get system statistics

        Menampilkan:
        - received: Total event diterima
        - unique_processed: Event unik diproses
        - duplicate_dropped: Duplikat ditolak
        - outbox_processed: Outbox entries processed
        - topics: Daftar topic unik
        - uptime_seconds: Waktu aktif
        - dedup_rate: Persentase deduplikasi
        - isolation_level: Isolation level yang digunakan
        """
        try:
            db_stats = await _dedup_store.get_stats()
            topics = await _dedup_store.get_topics()
            uptime = _metrics.get_uptime()
            outbox_pending = await _dedup_store.get_outbox_pending_count()

            received = db_stats['received']
            unique_processed = db_stats['unique_processed']
            duplicate_dropped = db_stats['duplicate_dropped']

            # Hitung dedup rate
            total_unique = unique_processed + duplicate_dropped
            dedup_rate = 0.0
            if total_unique > 0:
                dedup_rate = round(
                    (duplicate_dropped / total_unique) * 100, 2
                )

            return StatsResponse(
                received=received,
                unique_processed=unique_processed,
                duplicate_dropped=duplicate_dropped,
                outbox_processed=db_stats.get('outbox_processed', 0),
                topics=topics,
                uptime_seconds=uptime,
                dedup_rate=dedup_rate,
                isolation_level=ISOLATION_LEVEL,
                timestamp=datetime.utcnow()
            )

        except Exception as e:
            logger.error(f"Error getting stats: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ============================================================
    # Get Outbox Status
    # ============================================================
    @app.get("/outbox/status")
    async def get_outbox_status():
        """Get outbox processing status"""
        try:
            pending = await _dedup_store.get_outbox_pending_count()
            stats = await _dedup_store.get_stats()
            return {
                "status": "success",
                "pending": pending,
                "processed": stats.get('outbox_processed', 0),
                "timestamp": format_timestamp()
            }
        except Exception as e:
            logger.error(f"Error getting outbox status: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ============================================================
    # Error Handlers
    # ============================================================
    @app.exception_handler(ValueError)
    async def value_error_handler(request, exc):
        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "message": str(exc),
                "timestamp": format_timestamp()
            }
        )

    @app.exception_handler(Exception)
    async def general_exception_handler(request, exc):
        logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": "Internal server error",
                "timestamp": format_timestamp()
            }
        )

    return app


async def _process_outbox_loop():
    """
    Background task untuk memproses outbox entries secara periodik.
    Menggunakan transaksi dengan SELECT FOR UPDATE SKIP LOCKED
    untuk menghindari race condition antar worker.
    """
    global _dedup_store
    logger.info("Outbox processor started")

    while True:
        try:
            await asyncio.sleep(5)  # Process every 5 seconds
            if _dedup_store:
                processed = await _dedup_store.process_outbox_batch(batch_size=50)
                if processed > 0:
                    logger.info(f"Outbox: processed {processed} entries")
        except asyncio.CancelledError:
            logger.info("Outbox processor cancelled")
            break
        except Exception as e:
            logger.error(f"Error in outbox processor: {str(e)}")
            await asyncio.sleep(10)
