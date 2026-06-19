import http from "k6/http";
import { check, sleep } from "k6";
import { Counter } from "k6/metrics";
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";

const BASE_URL  = __ENV.BASE_URL || "http://localhost:8080";
const DUP_RATE  = 0.35;
const POOL_SIZE = 500;

const dupCounter  = new Counter("duplicate_sent");
const uniqCounter = new Counter("unique_sent");

export const options = {
  scenarios: {
    main: {
      executor:    "shared-iterations",
      vus:         50,
      iterations:  200,   // 50 VU × 200 = 10.000 batch @ 100 event = 20.000 event
      maxDuration: "5m",
    },
  },
  thresholds: {
    http_req_duration: ["p(95)<500"],
    http_req_failed:   ["rate<0.01"],
  },
};

const pool = Array.from({ length: POOL_SIZE }, () => ({
  topic:    `topic.${Math.floor(Math.random() * 20).toString().padStart(2,"0")}`,
  event_id: uuidv4(),
}));

export default function () {
  const events = [];
  for (let i = 0; i < 100; i++) {
    let topic, event_id;
    if (Math.random() < DUP_RATE) {
      const pick = pool[Math.floor(Math.random() * pool.length)];
      topic    = pick.topic;
      event_id = pick.event_id;
      dupCounter.add(1);
    } else {
      topic    = `topic.${Math.floor(Math.random() * 20).toString().padStart(2,"0")}`;
      event_id = uuidv4();
      uniqCounter.add(1);
    }
    events.push({
      topic,
      event_id,
      timestamp: new Date().toISOString(),
      source:    "k6-load",
      payload:   { v: Math.random() },
    });
  }

  const res = http.post(
    `${BASE_URL}/publish`,
    JSON.stringify({ events }),
    { headers: { "Content-Type": "application/json" } }
  );
  check(res, { "status 202": (r) => r.status === 202 });
  sleep(0.05);
}

export function handleSummary(data) {
  return {
    stdout: JSON.stringify({
      p95_ms:       data.metrics.http_req_duration?.values?.["p(95)"],
      rps:          data.metrics.http_reqs?.values?.rate,
      error_rate:   data.metrics.http_req_failed?.values?.rate,
      total_events: data.metrics.http_reqs?.values?.count * 100,
    }, null, 2),
  };
}
