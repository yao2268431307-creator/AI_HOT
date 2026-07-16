from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import time
from urllib.parse import urlsplit
import uuid

import boto3
import psycopg
from redis import Redis


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_ENV = (
    "DOCKER_INTEGRATION_CONTEXT",
    "POSTGRES_INTEGRATION_DSN",
    "REDIS_INTEGRATION_URL",
    "S3_INTEGRATION_ENDPOINT",
    "S3_INTEGRATION_ACCESS_KEY",
    "S3_INTEGRATION_SECRET_KEY",
)
RECOVERY_TIMEOUT_SECONDS = 60
LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}


def require_loopback_service_url(name: str, value: str, schemes: set[str], port: int) -> None:
    parsed = urlsplit(value)
    try:
        actual_port = parsed.port
    except ValueError as exc:
        raise RuntimeError(f"{name} has an invalid port") from exc
    if parsed.scheme not in schemes or parsed.hostname not in LOOPBACK_HOSTS or actual_port != port:
        expected_schemes = "/".join(sorted(schemes))
        raise RuntimeError(
            f"{name} must target the local compose service at {expected_schemes}://localhost:{port}",
        )


def require_local_docker_context(context: str) -> None:
    inspection = subprocess.run(
        ["docker", "context", "inspect", context, "--format", "{{.Endpoints.docker.Host}}"],
        cwd=ROOT, check=False, capture_output=True, text=True, timeout=10,
    )
    endpoint = inspection.stdout.strip().lower()
    if inspection.returncode != 0:
        raise RuntimeError(f"cannot inspect Docker context {context!r}: {inspection.stderr.strip()}")
    if not endpoint.startswith(("npipe://", "unix://", "fd://")):
        raise RuntimeError(f"Docker context {context!r} is not local: {endpoint!r}")


def require_environment() -> dict[str, str]:
    values = {name: os.getenv(name, "") for name in REQUIRED_ENV}
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise RuntimeError(f"missing integration settings: {', '.join(missing)}")
    return values


def main() -> int:
    settings = require_environment()
    docker_context = settings["DOCKER_INTEGRATION_CONTEXT"]
    require_local_docker_context(docker_context)
    require_loopback_service_url(
        "POSTGRES_INTEGRATION_DSN", settings["POSTGRES_INTEGRATION_DSN"], {"postgres", "postgresql"}, 5432,
    )
    require_loopback_service_url("REDIS_INTEGRATION_URL", settings["REDIS_INTEGRATION_URL"], {"redis"}, 6379)
    require_loopback_service_url(
        "S3_INTEGRATION_ENDPOINT", settings["S3_INTEGRATION_ENDPOINT"], {"http", "https"}, 9000,
    )
    run_id = f"integration-recovery-{uuid.uuid4().hex}"
    stream = f"{run_id}:stream"
    bucket = run_id
    key = "raw/persistence.json"
    body = json.dumps({"runId": run_id, "persisted": True}, separators=(",", ":")).encode()
    redis = Redis.from_url(settings["REDIS_INTEGRATION_URL"], decode_responses=True)
    s3 = boto3.client(
        "s3", endpoint_url=settings["S3_INTEGRATION_ENDPOINT"],
        aws_access_key_id=settings["S3_INTEGRATION_ACCESS_KEY"],
        aws_secret_access_key=settings["S3_INTEGRATION_SECRET_KEY"], region_name="auto",
    )
    message_id = ""
    prepared = {"postgres": False, "redis": False, "s3": False}
    cleanup_errors: list[str] = []
    try:
        with psycopg.connect(settings["POSTGRES_INTEGRATION_DSN"]) as connection, connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO connector_status (id,payload) VALUES (%s,%s::jsonb)",
                (run_id, body.decode()),
            )
            connection.commit()
        prepared["postgres"] = True
        message_id = redis.xadd(stream, {"run_id": run_id, "persisted": "true"})
        prepared["redis"] = True
        s3.create_bucket(Bucket=bucket)
        prepared["s3"] = True
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")

        compose_environment = os.environ.copy()
        for name in ("COMPOSE_FILE", "COMPOSE_PROJECT_NAME", "COMPOSE_PATH_SEPARATOR", "DOCKER_CONTEXT", "DOCKER_HOST"):
            compose_environment.pop(name, None)
        recovery_started = time.monotonic()
        restart = subprocess.run(
            [
                "docker", "--context", docker_context, "compose",
                "--file", str(ROOT / "docker-compose.yml"),
                "--project-directory", str(ROOT), "--project-name", "ai_hot",
                "restart", "postgres", "redis", "object-store",
            ],
            cwd=ROOT, env=compose_environment, check=False, capture_output=True, text=True,
            timeout=RECOVERY_TIMEOUT_SECONDS,
        )
        if restart.returncode != 0:
            raise RuntimeError(f"compose restart failed: {restart.stderr.strip()}")

        deadline = recovery_started + RECOVERY_TIMEOUT_SECONDS
        readiness: dict[str, bool] = {"postgres": False, "redis": False, "s3": False}
        while time.monotonic() < deadline and not all(readiness.values()):
            try:
                readiness["redis"] = bool(redis.ping())
            except Exception:
                readiness["redis"] = False
            try:
                readiness["s3"] = s3.head_object(Bucket=bucket, Key=key)["ContentLength"] == len(body)
            except Exception:
                readiness["s3"] = False
            try:
                with psycopg.connect(settings["POSTGRES_INTEGRATION_DSN"]) as connection, connection.cursor() as cursor:
                    cursor.execute("SELECT payload->>'runId' FROM connector_status WHERE id=%s", (run_id,))
                    row = cursor.fetchone()
                readiness["postgres"] = row is not None and row[0] == run_id
            except Exception:
                readiness["postgres"] = False
            if not all(readiness.values()):
                time.sleep(1)
        if not all(readiness.values()):
            raise RuntimeError(f"services did not recover within {RECOVERY_TIMEOUT_SECONDS} seconds: {readiness}")

        stream_rows = redis.xrange(stream)
        persisted_body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        result = {
            "runId": run_id,
            "postgresPersisted": readiness["postgres"],
            "redisStreamPersisted": stream_rows == [
                (message_id, {"run_id": run_id, "persisted": "true"}),
            ],
            "s3ObjectPersisted": persisted_body == body,
            "recoverySecondsUpperBound": round(time.monotonic() - recovery_started, 3),
        }
        result["passed"] = all((
            result["postgresPersisted"], result["redisStreamPersisted"], result["s3ObjectPersisted"],
        ))
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result["passed"] else 1
    finally:
        if prepared["redis"]:
            try:
                redis.delete(stream)
            except Exception as exc:
                cleanup_errors.append(f"redis:{exc}")
        if prepared["s3"]:
            try:
                s3.delete_object(Bucket=bucket, Key=key)
                s3.delete_bucket(Bucket=bucket)
            except Exception as exc:
                cleanup_errors.append(f"s3:{exc}")
        if prepared["postgres"]:
            try:
                with psycopg.connect(settings["POSTGRES_INTEGRATION_DSN"]) as connection, connection.cursor() as cursor:
                    cursor.execute("DELETE FROM connector_status WHERE id=%s", (run_id,))
                    connection.commit()
            except Exception as exc:
                cleanup_errors.append(f"postgres:{exc}")
        redis.close()
        if cleanup_errors:
            raise RuntimeError(f"integration cleanup failed for {run_id}: {'; '.join(cleanup_errors)}")


if __name__ == "__main__":
    raise SystemExit(main())
