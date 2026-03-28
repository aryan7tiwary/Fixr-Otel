"""
mock_stream.py
Generates realistic mock logs and metrics for all services.
Also provides the Kafka consumer factory used in KAFKA_MODE.
"""

import random
import time
import json

# ── Service registry ────────────────────────────────────────────────────────
SERVICES = ["cart-service", "payment-service", "frontend", "worker", "db", "api"]

# ── Normal log templates ────────────────────────────────────────────────────
NORMAL_LOGS = [
    "POST /cart/add received - user_id={uid}, item={item}",
    "GET /products returned 200 in {ms}ms",
    "DB query completed in {ms}ms",
    "Payment processed successfully for order={oid}",
    "Worker job {jid} completed",
    "Health check passed",
    "Cache hit ratio 0.{pct}",
    "Session {sid} authenticated",
]

# ── Anomalous log templates ─────────────────────────────────────────────────
ANOMALY_LOGS = [
    "ERROR: connection timeout to cart-db after 5000ms",
    "CRITICAL: upstream cart-service timeout after 5000ms",
    "ERROR: payment-service unreachable, request aborted",
    "FATAL: out of memory - heap exhausted",
    "ERROR: too many open connections to database",
    "WARN: retry attempt 5/5 failed - circuit open",
    "ERROR: connection refused localhost:5432",
    "CRITICAL: service health check failed repeatedly",
]

_ITEMS  = ["shoes", "shirt", "laptop", "watch", "bag"]
_random = random.Random()


def _rand_log(anomaly: bool) -> str:
    if anomaly:
        template = _random.choice(ANOMALY_LOGS)
    else:
        template = _random.choice(NORMAL_LOGS)
    return template.format(
        uid=_random.randint(1, 9999),
        item=_random.choice(_ITEMS),
        ms=_random.randint(5, 120),
        oid=_random.randint(10000, 99999),
        jid=_random.randint(1, 500),
        sid=_random.randint(1000, 9999),
        pct=_random.randint(70, 99),
    )


# ── Public generators ───────────────────────────────────────────────────────

def get_mock_log(force_anomaly_service: str = None) -> dict:
    """
    Return a single log dict.
    Shape (mirrors Kafka log topic): {service, log, timestamp}
    """
    service = _random.choice(SERVICES)
    is_anomaly = (service == force_anomaly_service)
    return {
        "service":   service,
        "log":       _rand_log(is_anomaly),
        "timestamp": time.time(),
    }


def get_mock_metrics(force_anomaly_service: str = None) -> list[dict]:
    """
    Return one metric dict per service.
    Shape (mirrors Kafka metrics topic): {service, timestamp, latency_ms, error_rate, cpu_percent}
    """
    results = []
    for svc in SERVICES:
        is_anomaly = (svc == force_anomaly_service)
        if is_anomaly:
            latency_ms  = _random.uniform(1500, 5000)
            error_rate  = _random.uniform(0.70, 1.00)
            cpu_percent = _random.uniform(75, 100)
        else:
            latency_ms  = _random.uniform(20, 150)
            error_rate  = _random.uniform(0.001, 0.02)
            cpu_percent = _random.uniform(10, 55)

        results.append({
            "service":     svc,
            "timestamp":   time.time(),
            "latency_ms":  round(latency_ms, 2),
            "error_rate":  round(error_rate, 4),
            "cpu_percent": round(cpu_percent, 2),
        })
    return results


# ── Kafka consumer factory ──────────────────────────────────────────────────

def get_kafka_consumer(brokers: str, topics: list[str]):
    """
    Returns a configured KafkaConsumer connected to Redpanda/Kafka.
    Messages are auto-decoded from JSON.
    Retries for 60 s to handle startup ordering in Docker Compose.
    """
    from kafka import KafkaConsumer
    import time as _time

    deadline = _time.time() + 60
    last_exc = None
    while _time.time() < deadline:
        try:
            consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=brokers,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",         # don't replay history on restart
                enable_auto_commit=True,
                group_id="ml-engine-consumer",
                session_timeout_ms=10_000,
                heartbeat_interval_ms=3_000,
            )
            return consumer
        except Exception as exc:
            last_exc = exc
            print(f"Kafka not ready ({exc}), retrying in 3 s …")
            _time.sleep(3)

    raise RuntimeError(f"Could not connect to Kafka at {brokers}: {last_exc}")