"""Thin MinIO/S3 client wrapper. MinIO is durable object storage for the
data lake (bronze/silver/gold Parquet, validation reports) — distinct
from Kafka (event *transport*, retained briefly) and PostgreSQL
(operational relational storage for `landing`/`curated`/control tables).
See ARCHITECTURE.md for the full distinction.
"""

from __future__ import annotations

import io
import json
import os
from typing import Any

import boto3
import pandas as pd
from amel_common.logging import get_logger
from botocore.client import Config as BotoConfig

logger = get_logger(component="object_store")

BUCKETS = ("bronze", "silver", "gold", "mlflow", "pipeline")

# boto3's client factory returns a dynamically-generated class with no
# static type, so annotating precisely would need the boto3-stubs
# package purely for typing; `Any` keeps this dependency-free.
S3Client = Any


def get_s3_client() -> S3Client:
    endpoint_url = os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000")
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "amel"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "amel_dev_password"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        config=BotoConfig(signature_version="s3v4"),
    )


def ensure_buckets(client: S3Client) -> None:
    existing = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
    for bucket in BUCKETS:
        if bucket not in existing:
            client.create_bucket(Bucket=bucket)
            logger.info("bucket_created", bucket=bucket)


def put_parquet(client: S3Client, bucket: str, key: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    logger.info("parquet_written", bucket=bucket, key=key, rows=len(df))


def get_parquet(client: S3Client, bucket: str, key: str) -> pd.DataFrame:
    obj = client.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()), engine="pyarrow")


def put_json(client: S3Client, bucket: str, key: str, data: dict[str, Any]) -> None:
    body = json.dumps(data, default=str, indent=2).encode("utf-8")
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    logger.info("json_written", bucket=bucket, key=key, bytes=len(body))
