"""
log_scorer.py
DistilBERT zero-shot log anomaly scorer.

Pipeline:  raw log string → clean → tokenise → DistilBERT → score 0→1
No fine-tuning needed. Uses the pre-trained sentiment head as a proxy:
  POSITIVE label ≈ anomalous (error-like language)
  NEGATIVE label ≈ normal
"""

from transformers import pipeline as hf_pipeline
from utils.normaliser import clean_log

# ── Model ───────────────────────────────────────────────────────────────────
# Loaded once at module import.  CPU inference is fast enough (<3s budget).
print("[log_scorer] Loading DistilBERT … (first run downloads ~250 MB)")
_classifier = hf_pipeline(
    "text-classification",
    model="distilbert-base-uncased-finetuned-sst-2-english",
    truncation=True,
    max_length=128,
)
print("[log_scorer] DistilBERT ready.")


# ── Public API ───────────────────────────────────────────────────────────────

def score_log(raw_log: str) -> float:
    """
    Score a raw log string for anomaly likelihood.

    Steps:
      1. Clean the log (strip timestamps, IPs, IDs via normaliser)
      2. Run DistilBERT text-classification
      3. Map POSITIVE → high score (anomaly), NEGATIVE → low score (normal)

    Returns:
        float in [0.0, 1.0].  Values ≥ 0.80 are considered anomalous.

    Example:
        score_log("ERROR at 10:42 from 10.0.0.1: connection timeout cart-db")
        → 0.91
    """
    if not raw_log or not raw_log.strip():
        return 0.0

    cleaned = clean_log(raw_log)
    if not cleaned:
        return 0.0

    try:
        result = _classifier(cleaned)[0]
        label  = result["label"]   # 'POSITIVE' or 'NEGATIVE'
        conf   = result["score"]   # model's confidence in that label

        # POSITIVE (error-like) = anomaly score = conf
        # NEGATIVE (normal)     = anomaly score = 1 - conf
        score = conf if label == "POSITIVE" else (1.0 - conf)
        return round(float(score), 4)

    except Exception as exc:
        print(f"[log_scorer] Inference error: {exc}")
        return 0.0