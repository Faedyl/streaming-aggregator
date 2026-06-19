#!/usr/bin/env python3
"""
Publisher simulator: kirim event ke aggregator.
30–50% dari event adalah duplikat (acak dari pool yang sudah dikirim).
"""
import asyncio, json, random, uuid, os, sys
from datetime import datetime, timezone
import httpx

TARGET_URL = os.getenv("TARGET_URL", "http://aggregator:8080/publish")
COUNT      = int(os.getenv("COUNT", "20000"))
DUP_RATE   = float(os.getenv("DUP_RATE", "0.4"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
TOPICS     = [f"topic.{i:02d}" for i in range(20)]
SOURCES    = ["svc-a", "svc-b", "svc-c", "gateway", "worker"]

sent_ids: list[tuple[str, str]] = []  # pool (topic, event_id) untuk duplikasi

def make_event() -> dict:
    if sent_ids and random.random() < DUP_RATE:
        topic, event_id = random.choice(sent_ids)
    else:
        topic    = random.choice(TOPICS)
        event_id = str(uuid.uuid4())
    return {
        "topic":     topic,
        "event_id":  event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source":    random.choice(SOURCES),
        "payload":   {"value": random.randint(1, 10000), "seq": len(sent_ids)},
    }

async def run():
    async with httpx.AsyncClient(timeout=30) as client:
        total_sent = total_err = 0
        batch = []
        for _ in range(COUNT):
            ev = make_event()
            batch.append(ev)
            key = (ev["topic"], ev["event_id"])
            if key not in sent_ids:
                sent_ids.append(key)

            if len(batch) >= BATCH_SIZE:
                try:
                    r = await client.post(TARGET_URL, json={"events": batch})
                    r.raise_for_status()
                    total_sent += len(batch)
                except Exception as e:
                    total_err += len(batch)
                    print(f"ERROR: {e}", file=sys.stderr)
                batch = []

        if batch:
            try:
                r = await client.post(TARGET_URL, json={"events": batch})
                r.raise_for_status()
                total_sent += len(batch)
            except Exception as e:
                total_err += len(batch)

    print(json.dumps({"total_sent": total_sent, "errors": total_err, "dup_rate": DUP_RATE}))

if __name__ == "__main__":
    asyncio.run(run())
