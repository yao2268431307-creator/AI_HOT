from __future__ import annotations

import os
import uuid

from botocore.exceptions import ClientError
import pytest
from redis.asyncio import Redis

from radar.evidence_store import S3EvidenceStore


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv("REDIS_INTEGRATION_URL"), reason="set REDIS_INTEGRATION_URL for Redis integration")
async def test_redis_stream_group_round_trip() -> None:
    stream = f"integration:radar:{uuid.uuid4().hex}"
    group = "integration-consumers"
    redis = Redis.from_url(os.environ["REDIS_INTEGRATION_URL"], decode_responses=True)
    try:
        assert await redis.ping()
        message_id = await redis.xadd(stream, {"kind": "integration", "aggregate_id": "event-1"})
        await redis.xgroup_create(stream, group, id="0-0")
        batches = await redis.xreadgroup(group, "consumer-1", {stream: ">"}, count=1, block=1000)
        assert len(batches) == 1
        returned_stream, messages = batches[0]
        assert returned_stream == stream
        assert messages == [(message_id, {"kind": "integration", "aggregate_id": "event-1"})]
        assert await redis.xack(stream, group, message_id) == 1
        assert (await redis.xpending(stream, group))["pending"] == 0
    finally:
        await redis.delete(stream)
        await redis.aclose()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not all(os.getenv(name) for name in ("S3_INTEGRATION_ENDPOINT", "S3_INTEGRATION_ACCESS_KEY", "S3_INTEGRATION_SECRET_KEY")),
    reason="set S3_INTEGRATION_ENDPOINT/access/secret for S3-compatible integration",
)
async def test_s3_compatible_raw_evidence_put_read_and_delete() -> None:
    endpoint = os.environ["S3_INTEGRATION_ENDPOINT"]
    access_key = os.environ["S3_INTEGRATION_ACCESS_KEY"]
    secret_key = os.environ["S3_INTEGRATION_SECRET_KEY"]
    bucket = f"integration-{uuid.uuid4().hex}"
    key = "raw/source/item.json"
    reference = f"r2://{bucket}/{key}"
    store = S3EvidenceStore(endpoint, access_key, secret_key)
    store.client.create_bucket(Bucket=bucket)
    try:
        await store.put(reference, b'{"evidence":true}', "application/json")
        response = store.client.get_object(Bucket=bucket, Key=key)
        assert response["Body"].read() == b'{"evidence":true}'
        assert response["ContentType"] == "application/json"
        await store.delete_many([reference])
        with pytest.raises(ClientError) as error:
            store.client.head_object(Bucket=bucket, Key=key)
        assert error.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    finally:
        remaining = store.client.list_objects_v2(Bucket=bucket).get("Contents", [])
        if remaining:
            store.client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": item["Key"]} for item in remaining]},
            )
        store.client.delete_bucket(Bucket=bucket)
