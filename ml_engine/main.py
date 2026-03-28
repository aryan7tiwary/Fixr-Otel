"""
main.py
FastAPI AI Engine — entry point for the ML observability service.

Endpoints:
  GET  /health          — liveness probe
  GET  /rca/latest      — latest RCA result (called by remediation layer)
  GET  /status          — per-service anomaly scores (Grafana polling)
  POST /fault/{service} — inject a fault in mock mode (demo use)
  POST /fault/clear     — clear injected fault
  POST /feedback        — remediation outcome + threshold adjustment

Environment variables:
  KAFKA_MODE    = false (mock) | true (real Redpanda)
  KAFKA_BROKERS = host:port   (default: localhost:9092)
  GRAFANA_URL   = http://...  (optional, for annotation pusher)
  GRAFANA_TOKEN = Bearer token (optional)
"""

import time
import threading
import asyncio
import os
import httpx

from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from pydantic import BaseModel

from utils.mock_stream import get_mock_log, get_mock_metrics, SERVICES
from models.log_scorer import score_log
from models.metric_scorer import train, score_metrics
from models.rca_engine import (
    ingest,
    run_rca,
    get_latest_scores,
    set_threshold,
    get_threshold,
)

# ── Environment ──────────────────────────────────────────────────────────────
KAFKA_MODE    = os.getenv("KAFKA_MODE", "false").lower() == "true"
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
GRAFANA_URL   = os.getenv("GRAFANA_URL", "")          # e.g. http://grafana:3000
GRAFANA_TOKEN = os.getenv("GRAFANA_TOKEN", "")        # Bearer token

# ── Threshold / feedback constants ───────────────────────────────────────────
THRESHOLD_CEILING = 0.95
THRESHOLD_NUDGE   = 0.02

# ── Pydantic request models ───────────────────────────────────────────────────
class FeedbackRequest(BaseModel):
    service:             str
    baseline_latency:    float   # ms  — pre-anomaly normal
    baseline_error_rate: float   # 0–1 — pre-anomaly normal


# ── Grafana annotation pusher ─────────────────────────────────────────────────

async def push_grafana_annotation(service: str, confidence: float, ts: float) -> None:
    """
    Push a vertical annotation to Grafana at the moment of anomaly detection.
    Silently skips if GRAFANA_URL is not configured.

    Grafana Annotations API:
      POST /api/annotations
      Body: { time, text, tags }
      time = Unix milliseconds
    """
    if not GRAFANA_URL or not GRAFANA_TOKEN:
        return  # Grafana not configured — skip silently

    payload = {
        "time":     int(ts * 1000),          # Grafana expects milliseconds
        "text":     f"⚠️ Anomaly: {service} | confidence={confidence:.2f}",
        "tags":     ["ml-engine", "anomaly", service],
        "isRegion": False,
    }
    headers = {
        "Authorization": f"Bearer {GRAFANA_TOKEN}",
        "Content-Type":  "application/json",
    }
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(
                f"{GRAFANA_URL}/api/annotations",
                json=payload,
                headers=headers,
            )
            if resp.status_code not in (200, 201):
                print(f"[grafana] Annotation push failed: {resp.status_code} {resp.text}")
            else:
                print(f"[grafana] Annotation pushed for {service} @ confidence {confidence:.2f}")
    except Exception as exc:
        print(f"[grafana] Annotation push error: {exc}")


# ── Startup lifespan ──────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("=" * 60)
    print("ML Engine starting up …")
    print(f"  KAFKA_MODE    = {KAFKA_MODE}")
    print(f"  KAFKA_BROKERS = {KAFKA_BROKERS}")
    print(f"  GRAFANA_URL   = {GRAFANA_URL or '(not set)'}")
    print("=" * 60)

    # Collect 100 baseline metric samples and train both models
    print("[startup] Collecting baseline (100 samples) …")
    baseline = []
    for _ in range(100):
        baseline.extend(get_mock_metrics())   # 6 services × 100 = 600 samples
    train(baseline)                           # trains IForest + fits scaler
    print("[startup] Models ready.")

    # Start background stream loop
    thread = threading.Thread(target=stream_loop, daemon=True)
    thread.start()
    print("[startup] Stream loop started.")

    yield  # app runs here

    print("[shutdown] ML Engine shutting down.")


app = FastAPI(
    title="PS3 ML Engine",
    description="AI observability — anomaly detection, RCA, feedback loop",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Background stream loop ────────────────────────────────────────────────────

FAULT_SERVICE: str | None = None   # mock-mode fault injection target


def stream_loop() -> None:
    """
    Runs forever as a daemon thread.
    In KAFKA_MODE: consumes real messages from Redpanda.
    In mock mode:  generates synthetic data every second.
    """
    if KAFKA_MODE:
        _kafka_loop()
    else:
        _mock_loop()


def _kafka_loop() -> None:
    """Real Kafka/Redpanda consumer. Handles both `logs` and `metrics` topics."""
    from utils.mock_stream import get_kafka_consumer
    consumer = get_kafka_consumer(KAFKA_BROKERS, ["logs", "metrics"])
    print(f"[kafka] Connected to {KAFKA_BROKERS}")

    for message in consumer:
        topic = message.topic
        data  = message.value    # already decoded to dict by KafkaConsumer

        try:
            if topic == "logs":
                # Shape: {service, log, timestamp}
                svc = data.get("service", "unknown")
                ls  = score_log(data.get("log", ""))
                ingest(svc, ls, 0.0)

            elif topic == "metrics":
                # Shape: {service, timestamp, latency_ms, error_rate, cpu_percent}
                svc = data.get("service", "unknown")
                ms  = score_metrics(data)
                ingest(svc, 0.0, ms)

        except Exception as exc:
            print(f"[kafka] Processing error on topic={topic}: {exc}")


def _mock_loop() -> None:
    """Synthetic data generator — one tick per second."""
    while True:
        metrics_batch = get_mock_metrics(force_anomaly_service=FAULT_SERVICE)
        for m in metrics_batch:
            svc         = m["service"]
            force_anom  = FAULT_SERVICE if svc == FAULT_SERVICE else None
            log_data    = get_mock_log(force_anomaly_service=force_anom)

            ls = score_log(log_data["log"])
            ms = score_metrics(m)
            ingest(svc, ls, ms)

        time.sleep(1)


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health", tags=["ops"])
def health():
    """Liveness probe."""
    return {"status": "ok", "kafka_mode": KAFKA_MODE}


@app.get("/rca/latest", tags=["rca"])
def latest_rca():
    """
    Latest RCA result from the 30-second sliding window.
    Called by the remediation layer every tick.

    Response (anomaly):
        {status: "anomaly", root_cause: "cart-service",
         confidence: 0.91, affected: ["payment-service", "frontend"],
         total_anomalous: 3}

    Response (normal):
        {status: "normal", anomalous_services: 0}
    """
    return run_rca()


@app.get("/status", tags=["rca"])
def status():
    """
    Current anomaly scores for every service in the sliding window.
    Useful for Grafana dashboard panels and debugging.

    Response:
        {
          "cart-service":    {"log_score": 0.12, "metric_score": 0.87, "combined": 0.50},
          "payment-service": {"log_score": 0.05, "metric_score": 0.06, "combined": 0.055},
          ...
        }
    """
    scores = get_latest_scores()
    return {
        "threshold": get_threshold(),
        "services":  scores,
    }


@app.post("/fault/{service}", tags=["demo"])
def inject_fault(service: str):
    """
    Inject an artificial fault into a service (mock mode only).
    Used during the demo: simulates docker stop <service>.
    """
    global FAULT_SERVICE
    if service not in SERVICES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown service '{service}'. Valid: {SERVICES}",
        )
    FAULT_SERVICE = service
    return {"status": "fault_injected", "service": service}


@app.post("/fault/clear", tags=["demo"])
def clear_fault():
    """Clear any injected fault (mock mode)."""
    global FAULT_SERVICE
    FAULT_SERVICE = None
    return {"status": "cleared"}


@app.post("/feedback", tags=["feedback"])
async def feedback(req: FeedbackRequest):
    """
    Called by the remediation layer after restarting a service.

    Flow:
      1. Validate the service name.
      2. Wait 10 s (recovery window).
      3. Read current combined anomaly score from the RCA window.
      4. Score < threshold  → TRUE POSITIVE  (remediation worked).
      5. Score ≥ threshold  → FALSE POSITIVE  (misfired, nudge threshold up).
      6. Push Grafana annotation marking the verdict.

    Body:
        { "service": "cart-service",
          "baseline_latency": 43.0,
          "baseline_error_rate": 0.001 }

    Response:
        { "service": "cart-service",
          "verdict": "true_positive",
          "score_after_10s": 0.31,
          "baseline": { "latency_ms": 43.0, "error_rate": 0.001 },
          "threshold": 0.80,
          "reason": "..." }
    """
    service = req.service

    if service not in SERVICES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown service '{service}'. Valid: {SERVICES}",
        )

    # ── Wait for service to (potentially) recover ────────────────────────────
    await asyncio.sleep(10)

    # ── Check scores ─────────────────────────────────────────────────────────
    scores = get_latest_scores()

    if service not in scores:
        return {
            "service":  service,
            "verdict":  "unknown",
            "reason":   (
                f"No data for '{service}' in the window after 10 s. "
                "Service may have stopped emitting events."
            ),
            "threshold": get_threshold(),
        }

    combined    = scores[service]["combined"]
    current_thr = get_threshold()

    # ── Label ─────────────────────────────────────────────────────────────────
    if combined < current_thr:
        verdict = "true_positive"
        reason  = (
            f"Score recovered to {combined:.3f} "
            f"(below threshold {current_thr:.2f}). "
            "Remediation was effective."
        )
        # No threshold change needed
        new_thr = current_thr

    else:
        # Still anomalous after restart — we misfired
        verdict = "false_positive"
        new_thr = min(current_thr + THRESHOLD_NUDGE, THRESHOLD_CEILING)
        set_threshold(new_thr)
        reason = (
            f"Score still {combined:.3f} after remediation "
            f"(above threshold {current_thr:.2f}). "
            f"Threshold nudged {current_thr:.2f} → {new_thr:.2f}."
        )

    # ── Push Grafana annotation for the verdict ───────────────────────────────
    await push_grafana_annotation(
        service    = service,
        confidence = combined,
        ts         = time.time(),
    )

    return {
        "service":        service,
        "verdict":        verdict,
        "score_after_10s": combined,
        "baseline": {
            "latency_ms":   req.baseline_latency,
            "error_rate":   req.baseline_error_rate,
        },
        "threshold":      new_thr,
        "reason":         reason,
    }