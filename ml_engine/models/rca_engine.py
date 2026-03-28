"""
rca_engine.py
Sliding-window Root Cause Analysis engine.

Design:
  - Module-level (not a class) so main.py can do simple
    `from models.rca_engine import ingest, run_rca, ...`
  - threading.Lock guards _scores against the daemon stream thread
    writing while FastAPI threads are reading.
  - Threshold is mutable so the feedback loop can nudge it up.
"""

import time
import threading
from collections import defaultdict

# ── Constants ────────────────────────────────────────────────────────────────
WINDOW_SECONDS    = 30
ANOMALY_THRESHOLD = 0.80

# ── Module-level state ───────────────────────────────────────────────────────
_lock      = threading.Lock()
_scores    = defaultdict(list)   # {service: [(ts, log_score, metric_score), ...]}
_threshold = ANOMALY_THRESHOLD   # mutable — nudged by feedback loop


# ── Threshold control ────────────────────────────────────────────────────────

def set_threshold(value: float) -> None:
    """Atomically update the anomaly threshold (called by feedback loop)."""
    global _threshold
    with _lock:
        _threshold = float(value)


def get_threshold() -> float:
    with _lock:
        return _threshold


# ── Ingestion ────────────────────────────────────────────────────────────────

def ingest(service: str, log_score: float, metric_score: float) -> None:
    """
    Append a scored event for a service and trim the 30-second window.
    Thread-safe: called from the background stream thread.
    """
    now    = time.time()
    cutoff = now - WINDOW_SECONDS
    with _lock:
        _scores[service].append((now, log_score, metric_score))
        _scores[service] = [
            (t, l, m) for t, l, m in _scores[service] if t > cutoff
        ]


# ── Per-service score snapshot ───────────────────────────────────────────────

def get_latest_scores() -> dict:
    """
    Return the most recent scores for every service in the window.
    Used by:
      - GET /status  (Grafana dashboard polling)
      - POST /feedback  (recovery check after 10s wait)

    Returns:
        {
          "cart-service":    {"log_score": 0.12, "metric_score": 0.87, "combined": 0.50},
          "payment-service": {...},
          ...
        }
    """
    with _lock:
        result = {}
        for service, events in _scores.items():
            if events:
                _, l, m = events[-1]        # most recent entry
                result[service] = {
                    "log_score":    round(l, 4),
                    "metric_score": round(m, 4),
                    "combined":     round((l + m) / 2, 4),
                }
        return result


# ── RCA ──────────────────────────────────────────────────────────────────────

def run_rca() -> dict:
    """
    Analyse the current 30-second window and find the root cause.

    Algorithm:
      1. Filter services whose max combined score ≥ threshold.
      2. Among anomalous services, find the one whose FIRST spike occurred
         earliest — that is the root cause (cascade origin).
      3. All other anomalous services are "affected" (cascade victims).

    Returns:
        Normal:  {"status": "normal", "anomalous_services": 0}
        Anomaly: {"status": "anomaly", "root_cause": str,
                  "confidence": float, "affected": list[str],
                  "total_anomalous": int}

    Example (from the reference doc walkthrough):
        cart-service spikes at T=1s → root_cause
        frontend spikes at T=3s    → affected
        payment  spikes at T=4s    → affected
    """
    with _lock:
        if not _scores:
            return {"status": "normal", "anomalous_services": 0}

        threshold = _threshold   # snapshot inside lock

        def combined_max(events):
            return max((l + m) / 2 for _, l, m in events)

        def first_spike_time(events):
            spikes = [t for t, l, m in events if (l + m) / 2 >= threshold]
            return min(spikes) if spikes else float("inf")

        # Services with at least one reading above threshold
        anomalous = {
            svc: events
            for svc, events in _scores.items()
            if events and combined_max(events) >= threshold
        }

        if not anomalous:
            return {"status": "normal", "anomalous_services": 0}

        # Earliest spike = root cause
        root       = min(anomalous, key=lambda s: first_spike_time(anomalous[s]))
        confidence = round(combined_max(anomalous[root]), 4)
        affected   = [s for s in anomalous if s != root]

        return {
            "status":          "anomaly",
            "root_cause":      root,
            "confidence":      confidence,
            "affected":        affected,
            "total_anomalous": len(anomalous),
        }