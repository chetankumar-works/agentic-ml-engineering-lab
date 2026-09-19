"""Reproducible DecisionTreeClassifier training for AMEL (Milestone 4).

`config` → `dataset` (Feast offline path) → `split` → `train` (fit,
evaluate, log to MLflow, register) → `promote` (explicit, audited
candidate → champion). `cli` wires them together; no notebook required.
"""
