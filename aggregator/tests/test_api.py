"""
Tests untuk API Endpoints
"""

import pytest
from fastapi.testclient import TestClient
from datetime import datetime
import os

# Skip if DATABASE_URL not set (for local dev without Docker)
pytestmark = pytest.mark.skipif(
    not os.getenv("DATABASE_URL"),
    reason="DATABASE_URL environment variable required"
)


@pytest.fixture(scope="module")
def client():
    """Create test client with real app"""
    from src.app import create_app
    app = create_app()
    return TestClient(app)


class TestHealthEndpoint:
    """Test /health endpoint"""

    def test_health_check(self, client):
        """Test health check endpoint"""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy" or data["status"] == "degraded"
        assert "database" in data
        assert "version" in data
        assert "timestamp" in data

    def test_liveness(self, client):
        """Test liveness probe"""
        response = client.get("/liveness")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "alive"
        assert "uptime_seconds" in data

    def test_readiness(self, client):
        """Test readiness probe"""
        response = client.get("/readiness")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "queue_length" in data


class TestPublishEndpoint:
    """Test /publish endpoint"""

    def test_publish_single_event(self, client):
        """Test publishing single event"""
        payload = {
            "events": [{
                "topic": "api-test",
                "event_id": "api-evt-001",
                "timestamp": datetime.utcnow().isoformat(),
                "source": "test-suite",
                "payload": {"test": "single"}
            }]
        }
        response = client.post("/publish", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert isinstance(data["received"], int)
        assert isinstance(data["processed"], int)
        assert isinstance(data["duplicates_detected"], int)

    def test_publish_batch(self, client):
        """Test publishing batch of events"""
        now = datetime.utcnow().isoformat()
        payload = {
            "events": [
                {
                    "topic": "api-batch",
                    "event_id": f"batch-evt-{i:03d}",
                    "timestamp": now,
                    "source": "test-suite",
                    "payload": {"index": i}
                }
                for i in range(20)
            ]
        }
        response = client.post("/publish", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["received"] == 20

    def test_publish_duplicate_in_batch(self, client):
        """
        Test bahwa dalam batch yang sama, duplikasi terdeteksi.

        Catatan: karena dedup cek ke database, event dengan event_id yang sama
        dalam batch yang sama hanya akan diproses sekali.
        """
        now = datetime.utcnow().isoformat()
        event_id = f"dup-in-batch-{datetime.utcnow().timestamp()}"

        # Kirim batch dengan duplikat event_id
        payload = {
            "events": [
                {
                    "topic": "dup-batch",
                    "event_id": event_id,
                    "timestamp": now,
                    "source": "test",
                    "payload": {"dup": 1}
                },
                {
                    "topic": "dup-batch",
                    "event_id": event_id,  # SAME event_id = duplicate
                    "timestamp": now,
                    "source": "test",
                    "payload": {"dup": 2}
                }
            ]
        }
        response = client.post("/publish", json=payload)
        assert response.status_code == 200
        data = response.json()
        # Setidaknya 1 duplikat terdeteksi (event ke-2)
        assert data["duplicates_detected"] >= 1

    def test_publish_invalid_missing_topic(self, client):
        """Test invalid event (missing topic)"""
        payload = {
            "events": [{
                "event_id": "no-topic",
                "timestamp": datetime.utcnow().isoformat(),
                "source": "test",
                "payload": {}
            }]
        }
        response = client.post("/publish", json=payload)
        assert response.status_code == 422

    def test_publish_invalid_missing_event_id(self, client):
        """Test invalid event (missing event_id)"""
        payload = {
            "events": [{
                "topic": "no-event-id",
                "timestamp": datetime.utcnow().isoformat(),
                "source": "test",
                "payload": {}
            }]
        }
        response = client.post("/publish", json=payload)
        assert response.status_code == 422

    def test_publish_empty_batch(self, client):
        """Test empty batch rejection"""
        payload = {"events": []}
        response = client.post("/publish", json=payload)
        assert response.status_code == 422


class TestEventsEndpoint:
    """Test /events endpoint"""

    def test_get_events(self, client):
        """Test getting events list"""
        response = client.get("/events")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "events" in data
        assert "count" in data
        assert isinstance(data["events"], list)

    def test_get_events_with_limit(self, client):
        """Test events with limit parameter"""
        response = client.get("/events?limit=5")
        assert response.status_code == 200
        data = response.json()
        assert len(data["events"]) <= 5

    def test_get_events_with_topic_filter(self, client):
        """Test events filtered by topic"""
        response = client.get("/events?topic=api-test")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data["events"], list)

    def test_get_events_invalid_limit_zero(self, client):
        """Test limit=0 is rejected"""
        response = client.get("/events?limit=0")
        assert response.status_code == 422

    def test_get_events_invalid_limit_exceeds(self, client):
        """Test limit > 1000 is rejected"""
        response = client.get("/events?limit=2000")
        assert response.status_code == 422


class TestStatsEndpoint:
    """Test /stats endpoint"""

    def test_get_stats(self, client):
        """Test getting statistics"""
        response = client.get("/stats")
        assert response.status_code == 200
        data = response.json()

        assert "received" in data
        assert "unique_processed" in data
        assert "duplicate_dropped" in data
        assert "outbox_processed" in data
        assert "topics" in data
        assert "uptime_seconds" in data
        assert "dedup_rate" in data
        assert "isolation_level" in data

        # Verify types
        assert isinstance(data["received"], int)
        assert isinstance(data["unique_processed"], int)
        assert isinstance(data["duplicate_dropped"], int)
        assert isinstance(data["outbox_processed"], int)
        assert isinstance(data["topics"], list)
        assert isinstance(data["uptime_seconds"], int)
        assert isinstance(data["dedup_rate"], (int, float))
        assert isinstance(data["isolation_level"], str)

    def test_get_outbox_status(self, client):
        """Test outbox status endpoint"""
        response = client.get("/outbox/status")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert "pending" in data
        assert "processed" in data


class TestIdempotency:
    """Test idempotency end-to-end through API"""

    def test_idempotent_publish(self, client):
        """
        Test idempotency: kirim event yang sama 3x,
        hanya pertama yang diproses.
        """
        now = datetime.utcnow().isoformat()
        event_id = f"idempotent-test-{datetime.utcnow().timestamp()}"

        payload = {
            "events": [{
                "topic": "idempotent",
                "event_id": event_id,
                "timestamp": now,
                "source": "test",
                "payload": {"msg": "idempotency test"}
            }]
        }

        # First send
        r1 = client.post("/publish", json=payload)
        d1 = r1.json()
        print(f"First send: processed={d1['processed']}, duplicates={d1['duplicates_detected']}")

        # Second send (same event)
        r2 = client.post("/publish", json=payload)
        d2 = r2.json()
        print(f"Second send: processed={d2['processed']}, duplicates={d2['duplicates_detected']}")

        # Third send (same event)
        r3 = client.post("/publish", json=payload)
        d3 = r3.json()
        print(f"Third send: processed={d3['processed']}, duplicates={d3['duplicates_detected']}")

        # First: processed=1, others: processed=0
        assert d1['processed'] >= 1, "First send should process the event"
        # Second onwards should have 0 processed (duplicate)
        assert d2['processed'] == 0, "Second send should detect duplicate"
        assert d2['duplicates_detected'] >= 1, "Second send should report duplicate"
        assert d3['processed'] == 0, "Third send should detect duplicate"
