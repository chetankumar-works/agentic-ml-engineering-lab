from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, Info

PREDICTIONS = Counter(
    "amel_inference_predictions_total",
    "Predictions served",
    ["source", "outcome"],  # source: raw|entity; outcome: ok|no_features|error
)
PREDICTION_LATENCY = Histogram(
    "amel_inference_prediction_latency_seconds",
    "End-to-end request latency for /predict/*",
    ["source"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)
FEATURE_FETCH_LATENCY = Histogram(
    "amel_inference_feature_fetch_seconds",
    "feast-server /get-online-features latency",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)
MODEL_INFO = Info("amel_inference_model", "Currently served model")
MODEL_LOADED = Gauge("amel_inference_model_loaded", "1 if a model is loaded")
MODEL_REFRESHES = Counter("amel_inference_model_refreshes_total", "Model swaps performed")
PERSIST_FAILURES = Counter(
    "amel_inference_persist_failures_total", "ml.predictions writes that failed"
)
PUBLISH_FAILURES = Counter(
    "amel_inference_publish_failures_total", "predictions.v1 deliveries that failed"
)
READY = Gauge("amel_inference_ready", "1 if /ready would return 200")
