"""Where generated events go: S3 for real runs, a local directory for inspection."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


def object_key(prefix: str, table: str, landed_at: datetime, seq: int) -> str:
    """Landing path.

    Partitioned by landing time, not event time. A late-arriving event therefore
    sits in a fresh partition carrying an old lsn, which is exactly the case
    bronze and silver have to survive.
    """
    day = landed_at.strftime("%Y-%m-%d")
    hour = landed_at.strftime("%H")
    stamp = int(landed_at.timestamp())
    return f"{prefix}/cdc/{table}/dt={day}/hh={hour}/part-{stamp}-{seq:04d}.json"


def serialize(records: list) -> bytes:
    """NDJSON. Malformed records are already strings and pass through untouched."""
    lines = [r if isinstance(r, str) else json.dumps(r, separators=(",", ":")) for r in records]
    return ("\n".join(lines) + "\n").encode("utf-8")


class LocalSink:
    def __init__(self, out_dir: str | Path):
        self.root = Path(out_dir)

    def write(self, prefix: str, table: str, records: list, landed_at: datetime, seq: int) -> str:
        path = self.root / object_key(prefix, table, landed_at, seq)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(serialize(records))
        return str(path)

    def describe(self) -> str:
        return f"local:{self.root}"


class S3Sink:
    def __init__(self, bucket: str, profile: str, region: str):
        import boto3

        self.bucket = bucket
        self.client = boto3.Session(profile_name=profile, region_name=region).client("s3")

    def write(self, prefix: str, table: str, records: list, landed_at: datetime, seq: int) -> str:
        key = object_key(prefix, table, landed_at, seq)
        # No SSE headers on purpose. The bucket has default SSE-KMS with the project
        # key, so objects are KMS encrypted without the generator needing the key id.
        # Passing ServerSideEncryption=aws:kms without a key id would silently switch
        # to the AWS-managed aws/s3 key instead.
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=serialize(records),
            ContentType="application/x-ndjson",
        )
        return f"s3://{self.bucket}/{key}"

    def describe(self) -> str:
        return f"s3://{self.bucket}"
