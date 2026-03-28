"""
metric_scorer.py
Isolation Forest metric anomaly scorer (PyOD).

Pipeline:  raw metric dict → z-score normalise → IForest → score 0→1

Train once at startup on ~100 baseline samples.
Score every incoming metric window in real time.
"""

import numpy as np
from pyod.models.iforest import IForest
from utils.normaliser import fit_scaler, scale_metrics, METRIC_FEATURES

# ── Model ────────────────────────────────────────────────────────────────────
# contamination=0.01 → we expect ~1% of baseline samples to be anomalous.
_model  = IForest(contamination=0.01, random_state=42)
_fitted = False


# ── Public API ───────────────────────────────────────────────────────────────

def train(baseline: list[dict]) -> None:
    """
    Fit the Isolation Forest on baseline metric samples.
    Also fits the z-score scaler (shared via normaliser module).

    Called once at startup from main.py lifespan.

    Args:
        baseline: list of metric dicts.  Each must contain the keys
                  defined in normaliser.METRIC_FEATURES:
                  ['latency_ms', 'error_rate', 'cpu_percent']

    Example:
        train([{"latency_ms": 43, "error_rate": 0.001, "cpu_percent": 28}, ...])
    """
    global _fitted

    # 1. Fit the shared z-score scaler
    fit_scaler(baseline)

    # 2. Build the scaled matrix and fit Isolation Forest
    X = np.array([
        [s.get(f, 0.0) for f in METRIC_FEATURES]
        for s in baseline
    ])
    # Use the already-fitted scaler from normaliser
    X_scaled = np.array([scale_metrics(s) for s in baseline])
    _model.fit(X_scaled)
    _fitted = True
    print(f"[metric_scorer] IForest trained on {len(baseline)} samples.")


def score_metrics(metric_dict: dict) -> float:
    """
    Score a single metric dict for anomaly likelihood.

    Steps:
      1. Z-score scale via normaliser.scale_metrics()
      2. Run Isolation Forest decision_function()
         (higher raw score = more normal)
      3. Invert and clip to [0, 1]

    Returns:
        float in [0.0, 1.0].  Values ≥ 0.80 are considered anomalous.

    Example:
        score_metrics({"latency_ms": 3200, "error_rate": 0.94, "cpu_percent": 92})
        → 0.97
    """
    if not _fitted:
        # Model not trained yet (called before lifespan completes) — return neutral
        return 0.0

    try:
        x_scaled = scale_metrics(metric_dict).reshape(1, -1)

        # decision_function: higher = more normal, lower = more anomalous
        raw = _model.decision_function(x_scaled)[0]

        # Normalise to [0, 1]: threshold_ is the decision boundary
        thresh = _model.threshold_
        # Below threshold = anomaly.  Map linearly, clip.
        # score = 1 when raw == thresh, rises further below thresh.
        anomaly_score = 1.0 - (raw / (thresh + 1e-9))
        return float(np.clip(anomaly_score, 0.0, 1.0))

    except Exception as exc:
        print(f"[metric_scorer] Scoring error: {exc}")
        return 0.0