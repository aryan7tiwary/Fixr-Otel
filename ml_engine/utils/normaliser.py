"""
normaliser.py
Two responsibilities:
  1. Log cleaning  — strips timestamps, IPs, IDs so the model sees
                     semantically identical strings as the same token sequence.
  2. Metric scaling — z-score normalises raw metric dicts so all features
                      sit on a comparable 0→1 scale before model inference.
"""

import re
import numpy as np
from sklearn.preprocessing import StandardScaler

# ── Metric feature order (must match metric_scorer.py) ─────────────────────
METRIC_FEATURES = ["latency_ms", "error_rate", "cpu_percent"]

# Module-level scaler — fit_scaler() trains it once at startup
_scaler: StandardScaler = StandardScaler()
_scaler_fitted: bool = False


# ── Log cleaning ────────────────────────────────────────────────────────────

# Patterns to strip (order matters)
_PATTERNS = [
    (r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?", ""),  # ISO timestamps
    (r"\d{2}:\d{2}:\d{2}(?:\.\d+)?", ""),                # HH:MM:SS
    (r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", ""),    # IPv4
    (r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", ""),  # UUIDs
    (r"\buser_id=\d+\b", ""),                             # user IDs
    (r"\border[_-]?id=\d+\b", ""),                        # order IDs
    (r"\bjob[_-]?id=\d+\b", ""),                          # job IDs
    (r"\bsession[_-]?id=\w+\b", ""),                      # session IDs
    (r"\b\d{4,}\b", ""),                                   # standalone long numbers
    (r"\s{2,}", " "),                                      # collapse whitespace
]
_COMPILED = [(re.compile(p, re.IGNORECASE), r) for p, r in _PATTERNS]


def clean_log(raw: str) -> str:
    """
    Strip volatile tokens so semantically identical errors
    produce identical token sequences for DistilBERT.

    Example:
        'ERROR at 10:42:01 from 192.168.1.5: connection timeout'
        → 'error connection timeout'
    """
    text = raw
    for pattern, replacement in _COMPILED:
        text = pattern.sub(replacement, text)
    return text.lower().strip()


# ── Metric scaling ──────────────────────────────────────────────────────────

def fit_scaler(baseline: list[dict]) -> None:
    """
    Fit the z-score scaler on baseline metric samples.
    Called once at startup from main.py.

    Args:
        baseline: list of metric dicts, each with keys in METRIC_FEATURES
    """
    global _scaler_fitted
    X = _extract_matrix(baseline)
    _scaler.fit(X)
    _scaler_fitted = True
    print(f"[normaliser] Scaler fitted on {len(baseline)} baseline samples.")


def scale_metrics(metric_dict: dict) -> np.ndarray:
    """
    Z-score scale a single metric dict.
    Returns a 1-D numpy array clipped to [0, 1].

    Raises RuntimeError if fit_scaler() has not been called yet.
    """
    if not _scaler_fitted:
        raise RuntimeError("Call fit_scaler() before scale_metrics().")
    row = np.array([[metric_dict.get(f, 0.0) for f in METRIC_FEATURES]])
    scaled = _scaler.transform(row)[0]
    # Sigmoid-style clip: map z-scores into (0, 1)
    clipped = 1 / (1 + np.exp(-scaled / 3))   # soft clip via sigmoid
    return clipped


def _extract_matrix(samples: list[dict]) -> np.ndarray:
    """Convert a list of metric dicts to a 2-D numpy array."""
    return np.array([
        [s.get(f, 0.0) for f in METRIC_FEATURES]
        for s in samples
    ])