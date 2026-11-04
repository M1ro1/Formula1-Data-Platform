"""Raw-file archive on S3-compatible object storage.

Environment variables
---------------------

ARCHIVE_ENABLED       "true" to archive during loads (default: off, so the loader works without storage)
ARCHIVE_ENDPOINT_URL  e.g. http://minio:9000 ; leave empty for real AWS S3
ARCHIVE_BUCKET        bucket name (default: f1-raw)
ARCHIVE_ACCESS_KEY / ARCHIVE_SECRET_KEY
ARCHIVE_REGION        default: us-east-1

"""
from __future__ import annotations

import logging
import os
from pathlib import Path

log = logging.getLogger(__name__)

_NOT_FOUND_CODES = {"404", "NoSuchBucket", "NoSuchKey", "NotFound"}


def is_enabled() -> bool:
    return os.getenv("ARCHIVE_ENABLED", "false").strip().lower() in ("1", "true", "yes")


def _bucket() -> str:
    return os.getenv("ARCHIVE_BUCKET", "f1-raw")


def _client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=os.getenv("ARCHIVE_ENDPOINT_URL") or None,
        aws_access_key_id=os.environ["ARCHIVE_ACCESS_KEY"],
        aws_secret_access_key=os.environ["ARCHIVE_SECRET_KEY"],
        region_name=os.getenv("ARCHIVE_REGION", "us-east-1"),
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )


def ensure_bucket(client=None) -> None:
    """Create the bucket if it does not exist yet."""
    from botocore.exceptions import ClientError

    client = client or _client()
    bucket = _bucket()
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in _NOT_FOUND_CODES:
            raise
        region = os.getenv("ARCHIVE_REGION", "us-east-1")
        if region != "us-east-1" and not os.getenv("ARCHIVE_ENDPOINT_URL"):
            client.create_bucket(
                Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": region}
            )
        else:
            client.create_bucket(Bucket=bucket)
        log.info("Created bucket %s", bucket)


def upload_file(path: Path | str, key: str) -> None:
    client = _client()
    ensure_bucket(client)
    client.upload_file(str(path), _bucket(), key)
    log.info("Archived %s -> s3://%s/%s", Path(path).name, _bucket(), key)


def download_file(key: str, dest: Path | str) -> Path:
    from botocore.exceptions import ClientError

    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        _client().download_file(_bucket(), key, str(dest))
    except ClientError as exc:
        if exc.response["Error"]["Code"] in _NOT_FOUND_CODES:
            raise FileNotFoundError(f"Not in the archive: s3://{_bucket()}/{key}") from exc
        raise
    log.info("Restored s3://%s/%s -> %s", _bucket(), key, dest)
    return dest


def list_keys(prefix: str) -> list[str]:
    """All object keys under a prefix (handles pagination)."""
    client = _client()
    keys: list[str] = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=_bucket(), Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys
