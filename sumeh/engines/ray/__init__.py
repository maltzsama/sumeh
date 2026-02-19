"""
Ray Data Engine - ML/AI Data Quality Validation.

Ray Data is the distributed data processing library for Ray ecosystem.
Designed for ML/AI workloads with native GPU support and Python-first API.

⚠️  ROW-LEVEL ONLY (No Aggregations)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Ray Data focuses on map/filter operations for ML pipelines.
Like PyFlink, we support row-level validations only.

Supported Validations (20 rules):
    ✅ Completeness: is_complete, are_complete
    ✅ Comparison: is_equal, is_greater_than, is_less_than, is_positive, is_negative
    ✅ Membership: is_in, not_in
    ✅ Pattern: has_pattern, is_legit
    ✅ Date checks: is_today, is_past_date, is_future_date, is_on_weekday

NOT Supported:
    ❌ Uniqueness (no groupBy in Ray Data)
    ❌ Table-level aggregations (limited agg API)
    ❌ Cross-batch validations

Key Concepts:
    - Uses .map_batches() with Pandas (FAST!)
    - Returns ValidatedRayDataset with .split()
    - GPU-accelerated validation possible
    - Native ML/AI integration (PyTorch, TensorFlow)

Why Ray Data?
    - Python-native (no JVM overhead)
    - GPU support (unique!)
    - ML/AI focused (training + inference)
    - Distributed (scales to 1000s nodes)
    - Zero-copy to PyTorch/TF

Why NOT Ray Data?
    - Limited aggregations (no groupBy)
    - Smaller community than Spark
    - Newer/less mature

Alternatives:
    - PySpark (mature, full aggregations)
    - Dask (scientific computing)
    - Polars (local, fast)

Example - Training Data Validation:
    >>> import ray
    >>> from sumeh import ray_data as sumeh_ray
    >>>
    >>> # Load training data
    >>> ds = ray.data.read_parquet("s3://bucket/training_data/")
    >>>
    >>> # Validate BEFORE training
    >>> validated = sumeh_ray.validate(ds, rules)
    >>> good_ds, bad_ds = validated.split()
    >>>
    >>> # Train only on clean data
    >>> from ray.train import TorchTrainer
    >>> trainer = TorchTrainer(...)
    >>> trainer.fit(good_ds)
    >>>
    >>> # Analyze rejected data
    >>> bad_ds.write_parquet("s3://bucket/quarantine/")

Example - Real-time Inference Validation:
    >>> import ray.serve
    >>>
    >>> @ray.serve.deployment
    >>> class Predictor:
    >>>     def predict(self, batch):
    >>>         # Validate input
    >>>         validated = sumeh_ray.validate(batch, rules)
    >>>         clean, bad = validated.split()
    >>>
    >>>         # Only predict on valid data
    >>>         return model.predict(clean)

Example - GPU-Accelerated Validation:
    >>> # Validate with GPU (for heavy regex/compute)
    >>> validated = sumeh_ray.validate(
    >>>     ds,
    >>>     rules,
    >>>     num_gpus=1  # GPU per batch
    >>> )

Example - Feature Store Validation:
    >>> # Validate features before storing
    >>> features = ray.data.read_parquet("computed_features/")
    >>> validated = sumeh_ray.validate(features, rules)
    >>> clean, _ = validated.split()
    >>>
    >>> # Only store valid features
    >>> clean.write_parquet("feature_store/")

Installation:
    pip install ray[data]

Performance Tips:
    - Use batch_size tuning (default 1024)
    - Enable GPU for heavy validation
    - Use .map_batches() internally (automatic)
    - Tune num_cpus per batch

Garbage In → Garbage Out:
    Your ML model is only as good as your training data.
    Validate with Sumeh before training/inference.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🚀 FIRST Data Quality framework built for Ray Data and ML pipelines!
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from sumeh.engines.ray_data.engine import validate

__all__ = ["validate"]
