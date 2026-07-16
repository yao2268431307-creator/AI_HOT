from __future__ import annotations

from datetime import datetime, timedelta, timezone
import base64
import hashlib
import importlib.util
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


MODULE_PATH = Path(__file__).parents[3] / "tools" / "acceptance_monitor.py"
SPEC = importlib.util.spec_from_file_location("acceptance_monitor", MODULE_PATH)
assert SPEC and SPEC.loader
monitor = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(monitor)


UTC = timezone.utc
CONNECTORS = ["rss", "hackernews", "github", "huggingface", "arxiv", "openalex"]
FAMILIES = {"rss": "official", "hackernews": "discussion", "github": "behavior", "huggingface": "behavior", "arxiv": "research", "openalex": "research"}
POLICY_DIGEST = "sha256:" + "a" * 64
POLICY_FROZEN_AT = datetime.now(UTC) - timedelta(days=30)
MONITOR_DIGEST = "sha256:" + hashlib.sha256(MODULE_PATH.read_bytes()).hexdigest()
SCHEMA_DIGEST = "sha256:" + hashlib.sha256((Path(__file__).parents[3] / "config" / "manual_product_evaluation.schema.json").read_bytes()).hexdigest()
SCHEDULER_KEY_ID = "scheduler-test-key"
SCHEDULER_PRIVATE = Ed25519PrivateKey.generate()
SCHEDULER_PRIVATE_BYTES = SCHEDULER_PRIVATE.private_bytes(
    serialization.Encoding.Raw, serialization.PrivateFormat.Raw, serialization.NoEncryption(),
)
SCHEDULER_PUBLIC_KEYS = {
    SCHEDULER_KEY_ID: SCHEDULER_PRIVATE.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw,
    ),
}
REVIEWER_PRIVATE_KEYS = {reviewer: Ed25519PrivateKey.generate() for reviewer in ("reviewer-a", "reviewer-b")}
REVIEWER_PUBLIC_KEYS = {
    reviewer: key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    for reviewer, key in REVIEWER_PRIVATE_KEYS.items()
}
BASELINE_KEY_ID = "baseline-test-key"
BASELINE_PRIVATE = Ed25519PrivateKey.generate()
BASELINE_PRIVATE_BYTES = BASELINE_PRIVATE.private_bytes(
    serialization.Encoding.Raw, serialization.PrivateFormat.Raw, serialization.NoEncryption(),
)
BASELINE_PUBLIC_KEYS = {
    BASELINE_KEY_ID: BASELINE_PRIVATE.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw,
    ),
}
LEDGER_KEY_ID = "score-ledger-test-key"
LEDGER_PRIVATE = Ed25519PrivateKey.generate()
LEDGER_PUBLIC_KEYS = {
    LEDGER_KEY_ID: LEDGER_PRIVATE.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw,
    ),
}
SERIES_START = datetime.combine((datetime.now(UTC) - timedelta(days=10)).date(), datetime.min.time(), tzinfo=UTC) + timedelta(hours=1)
TEST_KEYRING_IDENTITY = {
    "keyringVersion": "acceptance-keys-test-v1",
    "keyringDigest": "sha256:" + "b" * 64,
    "keyringFrozenAt": POLICY_FROZEN_AT.isoformat(),
}


def ranking_ledger(at: datetime) -> dict[str, object]:
    rows = [
        {
            "eventId": f"event-ledger-{rank:02d}", "score": 101 - rank,
            "scoreRunId": f"score-run-ledger-{rank:02d}", "scoreRunAt": SERIES_START.isoformat(),
            "firstDetectedAt": (SERIES_START - timedelta(hours=1)).isoformat(),
            "lifecycleState": "emerging", "coverage": 80,
            "eligibleTop5": True, "rank": rank,
        }
        for rank in range(1, 36)
    ]
    artifact: dict[str, object] = {
        "schemaVersion": "signed-score-ledger-v1",
        "productMetricPolicyVersion": "product-metrics-2026-07-rc2.7",
        "thresholdVersion": "thresholds-2026-07-rc2",
        "selectionRuleVersion": "daily-top5-score-v2",
        "generatedAt": at.isoformat(), "rows": rows,
        "leadCrossings": [
            {
                "eventId": f"event-ledger-{rank:02d}", "crossedAt": SERIES_START.isoformat(),
                "scoreRunId": f"score-run-ledger-{rank:02d}", "thresholdVersion": "thresholds-2026-07-rc2",
                "policyVersion": "product-metrics-2026-07-rc2.7",
            }
            for rank in range(1, 36)
        ] + [
            {
                "eventId": "event-pre-window-crossing", "crossedAt": (SERIES_START - timedelta(days=1)).isoformat(),
                "scoreRunId": "score-run-pre-window", "thresholdVersion": "thresholds-2026-07-rc2",
                "policyVersion": "product-metrics-2026-07-rc2.7",
            },
            {
                "eventId": "event-crossed-then-superseded", "crossedAt": SERIES_START.isoformat(),
                "scoreRunId": "score-run-superseded", "thresholdVersion": "thresholds-2026-07-rc2",
                "policyVersion": "product-metrics-2026-07-rc2.7",
            },
        ],
        "ledgerKeyId": LEDGER_KEY_ID,
    }
    material = monitor.signature_material(artifact, set())
    artifact["ledgerSignature"] = base64.b64encode(LEDGER_PRIVATE.sign(material)).decode()
    artifact["ledgerDigest"] = "sha256:" + hashlib.sha256(material).hexdigest()
    return artifact


def sample(at: datetime, *, beta_passes: bool = False) -> dict[str, object]:
    connector_rows = [
        {
            "connectorId": connector_id, "startedAt": at.isoformat(), "finishedAt": (at + timedelta(seconds=1)).isoformat(),
            "status": "healthy", "inserted": 10, "duplicates": 0, "latencyMs": 1000,
        }
        for connector_id in CONNECTORS
    ]
    return {
        "observedAt": at.isoformat(),
        "evidenceWindowHours": 2160,
        "sampleHealthy": True,
        "responses": {
            "health": {
                "status": "ok", "storageBackend": "postgresql", "rlsVerified": True,
                "authRequired": True, "productionReady": True, "migrationVersion": "001_init_rc2.3",
                "instanceId": "test-postgres-instance", "databaseClockSkewSeconds": .1,
                "databaseUser": "radar_app", "databaseRoleSuperuser": False,
                "databaseRoleBypassRls": False,
                "auditTriggersVerified": True, "migrationMarkerReadOnly": True,
            },
            "coverage": {"connectors": [{"id": connector_id, "family": FAMILIES[connector_id], "rightsStatus": "active"} for connector_id in CONNECTORS]},
            "connectorRuns": {"items": connector_rows},
            "pipelineSla": {
                "evidenceStatus": "eligible", "passesTarget": True, "rate": .99,
                "unrecoveredFailures": 0, "invalidFutureTimestamps": 0, "invalidTimestampOrder": 0,
            },
            "dataQuality": {
                "evidenceStatus": "eligible", "passesTarget": True, "sample": 100,
                "persistedDuplicates": 0, "rate": 0,
            },
            "betaMetrics": {
                "evidenceStatus": "eligible" if beta_passes else "insufficient",
                "passesMeasuredGates": True if beta_passes else None,
                "productMetricPolicyVersion": "product-metrics-2026-07-rc2.7",
                "productMetricPolicyDigest": POLICY_DIGEST,
                "policyStatus": "frozen_for_beta_collection",
                "policyFrozenAt": POLICY_FROZEN_AT.isoformat(),
                "asOf": at.isoformat(),
                "collectionWindow": {"from": POLICY_FROZEN_AT.isoformat(), "to": at.isoformat()},
                "manualEvaluationPolicy": {
                    "manualSchemaVersion": "manual-product-evaluation-v1.3", "manualSchemaDigest": SCHEMA_DIGEST,
                    "preregistrationSchemaVersion": "manual-product-preregistration-v2",
                    "preregistrationSchemaDigest": "sha256:" + hashlib.sha256(
                        (Path(__file__).parents[3] / "config" / "manual_product_preregistration.schema.json").read_bytes()
                    ).hexdigest(),
                    "snapshotTimezone": "Asia/Shanghai", "snapshotLocalTime": "09:00:00",
                    "snapshotTimeToleranceMinutes": 5,
                    "requiredThresholdVersion": "thresholds-2026-07-rc2",
                    "precisionAt5Target": .70, "precisionAt5MinimumCiLow": .70,
                    "minimumSnapshots": 7, "candidatesPerSnapshot": 5,
                    "intervalMethod": "bootstrap", "confidenceLevel": .95,
                    "minimumBootstrapIterations": 1000, "minimumDiscoveryLeadEvents": 30,
                    "minimumMedianDiscoveryLeadMinutes": 30,
                    "leadEligibilityRuleVersion": "lead-threshold-v1",
                    "leadMinimumAttention": 60, "leadMinimumCoverage": 60,
                    "leadLifecycleStates": ["emerging", "accelerating", "established"],
                },
                "acceptanceMonitoringPolicy": {
                    **TEST_KEYRING_IDENTITY,
                    "monitorVersion": "acceptance-monitor-v1.1", "monitorDigest": MONITOR_DIGEST,
                    "cadenceSeconds": 900, "minimumDistinctConnectorFamilies": 4,
                    "requiredSignalFamilies": ["discussion", "behavior"],
                    "requireNonzeroConnectorObservations": True,
                },
                "factLedger": {
                    "version": "beta-fact-ledger-v1",
                    "counts": {
                        "alertDeliveries": 30, "feedback": 50, "queueEntries": 50,
                        "interactions": 100, "metricIncidents": 0, "invalidExclusionAttempts": 0,
                    },
                    "digests": {
                        "alertDeliveries": "sha256:" + "1" * 64,
                        "feedback": "sha256:" + "2" * 64,
                        "queueEntries": "sha256:" + "3" * 64,
                        "interactions": "sha256:" + "4" * 64,
                        "metricIncidents": "sha256:" + "5" * 64,
                    },
                },
                "strongAlertAcceptance": {"denominatorDeliveredStrongAlerts": 30},
                "erroneousStrongAlerts": {"workdayStrongAlerts": 30, "erroneousStrongAlerts": 2},
                "firstTriageSla": {"sample": 50},
                "activeReviewTime": {
                    "sample": 50, "formalMeasurementEligible": True,
                    "measurementVersion": "server-heartbeat-v2",
                },
                "serverObservedReviewTime": {
                    "sample": 50, "formalMeasurementEligible": True,
                    "measurementVersion": "server-heartbeat-v2",
                },
            },
            "rankingLedger": ranking_ledger(at),
        },
        "errors": {},
    }


def series(hours: int, cadence_minutes: int = 15, *, beta_passes: bool = False) -> list[dict[str, object]]:
    start = SERIES_START
    count = hours * 60 // cadence_minutes + 1
    result = [sample(start + timedelta(minutes=index * cadence_minutes), beta_passes=beta_passes) for index in range(count)]
    for index, row in enumerate(result):
        row["scheduledAt"] = row["observedAt"]
        row["collectorRunId"] = f"scheduler-run-{index:05d}"
        monitor.seal_sample(row, SCHEDULER_PRIVATE_BYTES, SCHEDULER_KEY_ID)
    return result


def formal_evaluate(samples: list[dict[str, object]], **kwargs: object) -> dict[str, object]:
    kwargs.setdefault("scheduler_public_keys", SCHEDULER_PUBLIC_KEYS)
    kwargs.setdefault("keyring_identity", TEST_KEYRING_IDENTITY)
    kwargs.setdefault("baseline_public_keys", BASELINE_PUBLIC_KEYS)
    kwargs.setdefault("ledger_public_keys", LEDGER_PUBLIC_KEYS)
    return monitor.evaluate_samples(samples, **kwargs)


def test_canary_never_becomes_duration_acceptance_evidence() -> None:
    report = monitor.evaluate_samples(series(0), mode="canary")
    assert report["status"] == "canary_only"
    assert report["passes"] is True
    assert report["acceptanceEligible"] is False
    assert monitor.validated_api_url("http://127.0.0.1:8017") == "http://127.0.0.1:8017"
    try:
        monitor.validated_api_url("http://example.com")
    except ValueError as exc:
        assert "HTTPS" in str(exc)
    else:
        raise AssertionError("remote cleartext monitoring must be rejected")


def test_72_hour_soak_requires_full_span_connector_continuity_and_pipeline_sla() -> None:
    report = formal_evaluate(series(72), mode="soak72h")
    assert report["status"] == "passed"
    assert report["acceptanceEligible"] is True
    assert report["passes"] is True
    assert report["monitoring"]["spanHours"] == 72
    assert all(value["passes"] for value in report["connectors"].values())
    assert report["pipelineSla"]["passes"] is True

    truncated = formal_evaluate(series(71), mode="soak72h")
    assert truncated["status"] == "insufficient"
    assert truncated["passes"] is False


def test_shadow_gate_requires_real_product_samples_and_independent_manual_evaluation() -> None:
    samples = series(168, beta_passes=True)
    missing_manual = formal_evaluate(
        samples, mode="shadow7d",
        reviewer_public_keys=REVIEWER_PUBLIC_KEYS,
    )
    assert missing_manual["acceptanceEligible"] is True
    assert missing_manual["passes"] is False

    snapshot_start = monitor.parse_time(str(samples[0]["observedAt"]))
    reviewer_ids = ["reviewer-a", "reviewer-b"]
    raw_snapshots = []
    for snapshot_index in range(7):
        scheduled_at = snapshot_start + timedelta(days=snapshot_index)
        ledger = next(
            row["responses"]["rankingLedger"] for row in samples
            if monitor.parse_time(str(row["observedAt"])) == scheduled_at
        )
        eligible_rows = [row for row in ledger["rows"] if row["eligibleTop5"]][:5]
        raw_snapshots.append({
            "snapshotId": f"snapshot-{snapshot_index:02d}", "scheduledAt": scheduled_at.isoformat(),
            "candidates": [
                {
                    "rank": rank, "eventId": row["eventId"], "score": row["score"],
                    "scoreRunId": row["scoreRunId"],
                    "labels": [{"reviewerId": reviewer_id, "relevant": rank <= 4} for reviewer_id in reviewer_ids],
                }
                for rank, row in enumerate(eligible_rows, 1)
            ],
            "_ledgerDigest": ledger["ledgerDigest"],
        })
    precision_candidate_event_ids = sorted({
        candidate["eventId"] for snapshot in raw_snapshots for candidate in snapshot["candidates"]
    })
    first_ledger_crossings = samples[0]["responses"]["rankingLedger"]["leadCrossings"]
    raw_events = [
        {
            "eventId": row["eventId"], "radarDetectedAt": row["crossedAt"],
            "baselineDetectedAt": (monitor.parse_time(str(row["crossedAt"])) + timedelta(minutes=42)).isoformat(),
            "scoreRunId": row["scoreRunId"], "thresholdVersion": "thresholds-2026-07-rc2",
            "baselineLogRef": f"baseline-log-{index:02d}",
        }
        for index, row in enumerate(first_ledger_crossings)
        if monitor.parse_time(str(row["crossedAt"])) >= snapshot_start
    ]
    eligible_lead_event_ids = sorted(row["eventId"] for row in raw_events)
    preregistered_at = (snapshot_start - timedelta(days=1)).isoformat()
    preregistration = {
        "schemaVersion": "manual-product-preregistration-v2",
        "productMetricPolicyVersion": "product-metrics-2026-07-rc2.7",
        "productMetricPolicyDigest": POLICY_DIGEST,
        "selectionRuleVersion": "daily-top5-score-v2",
        "inclusionRules": ["top five eligible radar candidates at each scheduled snapshot"],
        "exclusionRules": ["exclude candidates marked data_insufficient at snapshot time"],
        "snapshotScheduleId": "daily-0900-asia-shanghai",
        "snapshotTimezone": "Asia/Shanghai",
        "snapshotLocalTime": "09:00",
        "leadEligibilityRuleVersion": "lead-threshold-v1",
        "thresholdVersion": "thresholds-2026-07-rc2",
        "baselineDefinitionId": "independent-baseline-v1",
        "bootstrapSeed": 20260717,
        "bootstrapIterations": 5000,
        "bootstrapUnit": "daily_snapshot",
        "preregisteredAt": preregistered_at,
    }
    monitor.seal_preregistration(preregistration, SCHEDULER_PRIVATE_BYTES, SCHEDULER_KEY_ID)
    preregistration_digest = monitor.artifact_digest(preregistration)
    for snapshot in raw_snapshots:
        snapshot["preregistrationDigest"] = preregistration_digest
        snapshot["rankingRuleVersion"] = "daily-top5-score-v2"
        snapshot["rankingResponseDigest"] = snapshot.pop("_ledgerDigest")
        monitor.seal_snapshot(snapshot, SCHEDULER_PRIVATE_BYTES, SCHEDULER_KEY_ID)
    lead_manifest = {
        "schemaVersion": "manual-lead-candidate-manifest-v1",
        "preregistrationDigest": preregistration_digest,
        "generatedAt": monitor.parse_time(str(samples[-1]["observedAt"])).isoformat(),
        "candidates": [
            {
                "eventId": row["eventId"], "radarDetectedAt": row["radarDetectedAt"],
                "scoreRunId": row["scoreRunId"], "thresholdVersion": row["thresholdVersion"],
                "eligibilityReason": "first threshold crossing in signed score ledger",
            }
            for row in raw_events
        ],
    }
    monitor.seal_lead_manifest(lead_manifest, SCHEDULER_PRIVATE_BYTES, SCHEDULER_KEY_ID)
    baseline_artifact = {
        "schemaVersion": "manual-independent-baseline-v1",
        "generatedAt": monitor.parse_time(str(samples[-1]["observedAt"])).isoformat(),
        "eventLogs": [
            {
                "logRef": row["baselineLogRef"], "eventId": row["eventId"],
                "firstDetectedAt": row["baselineDetectedAt"],
            }
            for row in raw_events
        ],
    }
    monitor.seal_baseline_artifact(baseline_artifact, BASELINE_PRIVATE_BYTES, BASELINE_KEY_ID)
    manual = {
        "schemaVersion": "manual-product-evaluation-v1.3",
        "productMetricPolicyVersion": "product-metrics-2026-07-rc2.7",
        "productMetricPolicyDigest": POLICY_DIGEST,
        "precisionCandidateEventSetDigest": "sha256:" + hashlib.sha256(
            __import__("json").dumps(precision_candidate_event_ids, ensure_ascii=False, separators=(",", ":")).encode()
        ).hexdigest(),
        "precisionCandidateEventIds": precision_candidate_event_ids,
        "eligibleLeadEventIds": eligible_lead_event_ids,
        "eligibleLeadEventSetDigest": "sha256:" + hashlib.sha256(
            __import__("json").dumps(eligible_lead_event_ids, ensure_ascii=False, separators=(",", ":")).encode()
        ).hexdigest(),
        "snapshotScheduleId": "daily-0900-asia-shanghai",
        "preregisteredAt": preregistered_at,
        "evaluatedAt": monitor.parse_time(str(samples[-1]["observedAt"])).isoformat(),
        "reviewerIds": reviewer_ids,
        "reviewerSignatures": {},
        "baselineArtifact": baseline_artifact,
        "baselineArtifactDigest": monitor.artifact_digest(baseline_artifact),
        "precisionAt5": {
            "doubleReviewed": True, "snapshots": 7, "candidates": 35,
            "value": .8, "ciLow": .8, "ciHigh": .8,
            "intervalMethod": "bootstrap", "confidenceLevel": .95,
            "bootstrapIterations": 5000, "bootstrapSeed": 20260717,
            "rawSnapshots": raw_snapshots,
        },
        "discoveryLead": {"independentBaseline": True, "sample": len(raw_events), "medianMinutes": 42, "rawEvents": raw_events},
        "preregistrationArtifact": preregistration,
        "preregistrationDigest": preregistration_digest,
        "leadCandidateManifest": lead_manifest,
        "leadCandidateManifestDigest": monitor.artifact_digest(lead_manifest),
    }
    for row in samples:
        row["manualPreregistrationDigest"] = manual["preregistrationDigest"]
        monitor.seal_sample(row, SCHEDULER_PRIVATE_BYTES, SCHEDULER_KEY_ID)
    for snapshot in raw_snapshots:
        committed_sample = next(
            row for row in samples
            if monitor.parse_time(str(row["observedAt"])) == monitor.parse_time(snapshot["scheduledAt"])
        )
        committed_sample["manualSnapshotCommitmentDigest"] = monitor.snapshot_artifact_digest(snapshot)
        monitor.seal_sample(committed_sample, SCHEDULER_PRIVATE_BYTES, SCHEDULER_KEY_ID)
    material = monitor.signature_material(manual, {"reviewerSignatures"})
    manual["reviewerSignatures"] = {
        reviewer: base64.b64encode(REVIEWER_PRIVATE_KEYS[reviewer].sign(material)).decode()
        for reviewer in reviewer_ids
    }
    passed = formal_evaluate(
        samples, mode="shadow7d", manual_evaluation=manual,
        reviewer_public_keys=REVIEWER_PUBLIC_KEYS,
    )
    assert passed["status"] == "passed", (
        passed["scoreLedger"], passed["productMetrics"], passed["manualEvaluation"], passed["frozenPolicy"],
    )
    assert passed["passes"] is True
    assert passed["manualEvaluation"]["recomputed"]["precisionAt5"] == .8
    assert "event-pre-window-crossing" not in eligible_lead_event_ids
    assert "event-crossed-then-superseded" in eligible_lead_event_ids
    assert all(
        row["eventId"] != "event-crossed-then-superseded"
        for row in samples[0]["responses"]["rankingLedger"]["rows"]
    )

    tampered_ledger_samples = __import__("copy").deepcopy(samples)
    tampered_sample = tampered_ledger_samples[0]
    tampered_sample["responses"]["rankingLedger"]["rows"][0]["score"] = 999
    monitor.seal_sample(tampered_sample, SCHEDULER_PRIVATE_BYTES, SCHEDULER_KEY_ID)
    tampered_report = formal_evaluate(
        tampered_ledger_samples, mode="shadow7d", manual_evaluation=manual,
        reviewer_public_keys=REVIEWER_PUBLIC_KEYS,
    )
    assert tampered_report["scoreLedger"]["passes"] is False

    forged_summary = __import__("copy").deepcopy(manual)
    forged_summary["precisionAt5"]["value"] = .95
    forged_summary["precisionAt5"]["ciLow"] = .9
    forged_summary["precisionAt5"]["ciHigh"] = 1
    rejected = formal_evaluate(
        samples, mode="shadow7d", manual_evaluation=forged_summary,
        reviewer_public_keys=REVIEWER_PUBLIC_KEYS,
    )
    assert rejected["manualEvaluation"]["passes"] is False

    forged_snapshot = __import__("copy").deepcopy(manual)
    forged_snapshot["precisionAt5"]["rawSnapshots"][0]["candidates"][0]["score"] = 999
    forged_snapshot["reviewerSignatures"] = {}
    forged_material = monitor.signature_material(forged_snapshot, {"reviewerSignatures"})
    forged_snapshot["reviewerSignatures"] = {
        reviewer: base64.b64encode(REVIEWER_PRIVATE_KEYS[reviewer].sign(forged_material)).decode()
        for reviewer in reviewer_ids
    }
    assert formal_evaluate(
        samples, mode="shadow7d", manual_evaluation=forged_snapshot,
        reviewer_public_keys=REVIEWER_PUBLIC_KEYS,
    )["manualEvaluation"]["passes"] is False

    forged_top5 = __import__("copy").deepcopy(manual)
    forged_top5_samples = __import__("copy").deepcopy(samples)
    forged_daily = forged_top5["precisionAt5"]["rawSnapshots"][0]
    sixth = samples[0]["responses"]["rankingLedger"]["rows"][5]
    forged_daily["candidates"][4].update({
        "eventId": sixth["eventId"], "score": sixth["score"], "scoreRunId": sixth["scoreRunId"],
    })
    monitor.seal_snapshot(forged_daily, SCHEDULER_PRIVATE_BYTES, SCHEDULER_KEY_ID)
    forged_ids = sorted({
        candidate["eventId"]
        for snapshot in forged_top5["precisionAt5"]["rawSnapshots"]
        for candidate in snapshot["candidates"]
    })
    forged_top5["precisionCandidateEventIds"] = forged_ids
    forged_top5["precisionCandidateEventSetDigest"] = "sha256:" + hashlib.sha256(
        __import__("json").dumps(forged_ids, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    forged_committed_sample = forged_top5_samples[0]
    forged_committed_sample["manualSnapshotCommitmentDigest"] = monitor.snapshot_artifact_digest(forged_daily)
    monitor.seal_sample(forged_committed_sample, SCHEDULER_PRIVATE_BYTES, SCHEDULER_KEY_ID)
    forged_top5["reviewerSignatures"] = {}
    forged_material = monitor.signature_material(forged_top5, {"reviewerSignatures"})
    forged_top5["reviewerSignatures"] = {
        reviewer: base64.b64encode(REVIEWER_PRIVATE_KEYS[reviewer].sign(forged_material)).decode()
        for reviewer in reviewer_ids
    }
    assert formal_evaluate(
        forged_top5_samples, mode="shadow7d", manual_evaluation=forged_top5,
        reviewer_public_keys=REVIEWER_PUBLIC_KEYS,
    )["manualEvaluation"]["passes"] is False

    forged_baseline = __import__("copy").deepcopy(manual)
    forged_baseline["baselineArtifact"]["eventLogs"][0]["firstDetectedAt"] = snapshot_start.isoformat()
    forged_baseline["reviewerSignatures"] = {}
    forged_material = monitor.signature_material(forged_baseline, {"reviewerSignatures"})
    forged_baseline["reviewerSignatures"] = {
        reviewer: base64.b64encode(REVIEWER_PRIVATE_KEYS[reviewer].sign(forged_material)).decode()
        for reviewer in reviewer_ids
    }
    assert formal_evaluate(
        samples, mode="shadow7d", manual_evaluation=forged_baseline,
        reviewer_public_keys=REVIEWER_PUBLIC_KEYS,
    )["manualEvaluation"]["passes"] is False


def test_formal_monitor_rejects_time_schema_connector_and_policy_shortcuts() -> None:
    samples = series(168, beta_passes=True)
    future = [dict(row, observedAt=(monitor.parse_time(row["observedAt"]) + timedelta(days=30)).isoformat()) for row in samples]
    assert formal_evaluate(future, mode="shadow7d")["acceptanceEligible"] is False

    reversed_pair = samples.copy()
    reversed_pair[1], reversed_pair[2] = reversed_pair[2], reversed_pair[1]
    assert formal_evaluate(reversed_pair, mode="shadow7d")["acceptanceEligible"] is False

    try:
        formal_evaluate(samples, mode="shadow7d", cadence_seconds=3600)
    except ValueError as exc:
        assert "900" in str(exc)
    else:
        raise AssertionError("formal cadence must not be caller-adjustable")

    try:
        formal_evaluate(samples, mode="soak72h", required_connectors=["rss", "hackernews", "github"])
    except ValueError as exc:
        assert "four" in str(exc)
    else:
        raise AssertionError("at least four connector families are required")

    same_family = series(72)
    for row in same_family:
        for connector in row["responses"]["coverage"]["connectors"]:
            connector["family"] = "discussion"
    assert formal_evaluate(same_family, mode="soak72h")["signalFamilyCoverage"]["passes"] is False

    no_observations = series(72)
    for row in no_observations:
        for run in row["responses"]["connectorRuns"]["items"]:
            run["inserted"] = run["duplicates"] = 0
    assert formal_evaluate(no_observations, mode="soak72h")["passes"] is False

    changed_policy = series(72)
    changed_policy[len(changed_policy) // 2]["responses"]["betaMetrics"]["productMetricPolicyDigest"] = "sha256:" + "b" * 64
    assert formal_evaluate(changed_policy, mode="soak72h")["frozenPolicy"]["passes"] is False

    in_memory = series(72)
    for row in in_memory:
        row["responses"]["health"].update({
            "storageBackend": "in_memory", "rlsVerified": False, "productionReady": False,
        })
        monitor.seal_sample(row, SCHEDULER_PRIVATE_BYTES, SCHEDULER_KEY_ID)
    assert formal_evaluate(in_memory, mode="soak72h")["monitoring"]["runtimeAttestationPasses"] is False

    shrinking_ledger = series(72)
    middle = shrinking_ledger[len(shrinking_ledger) // 2]
    middle["responses"]["betaMetrics"]["factLedger"]["counts"]["feedback"] = 49
    middle["responses"]["betaMetrics"]["factLedger"]["digests"]["feedback"] = "sha256:" + "9" * 64
    monitor.seal_sample(middle, SCHEDULER_PRIVATE_BYTES, SCHEDULER_KEY_ID)
    assert formal_evaluate(shrinking_ledger, mode="soak72h")["productMetrics"]["factLedgerContinuityPasses"] is False

    shrinking_crossings = series(72)
    middle = shrinking_crossings[len(shrinking_crossings) // 2]
    artifact = middle["responses"]["rankingLedger"]
    artifact["leadCrossings"] = artifact["leadCrossings"][:-1]
    artifact.pop("ledgerSignature")
    artifact.pop("ledgerDigest")
    material = monitor.signature_material(artifact, set())
    artifact["ledgerSignature"] = base64.b64encode(LEDGER_PRIVATE.sign(material)).decode()
    artifact["ledgerDigest"] = "sha256:" + hashlib.sha256(material).hexdigest()
    monitor.seal_sample(middle, SCHEDULER_PRIVATE_BYTES, SCHEDULER_KEY_ID)
    crossing_report = formal_evaluate(shrinking_crossings, mode="soak72h")
    assert crossing_report["scoreLedger"]["leadCrossingContinuityPasses"] is False


def test_keyring_rejects_duplicate_or_cross_role_key_material(tmp_path: Path) -> None:
    public_key = next(iter(REVIEWER_PUBLIC_KEYS.values()))
    encoded = base64.b64encode(public_key).decode()
    keyring = {
        "schemaVersion": "acceptance-ed25519-keyring-v1",
        "keyringVersion": "duplicate-keyring-v1",
        "frozenAt": POLICY_FROZEN_AT.isoformat(),
        "schedulerKeys": [],
        "reviewerKeys": [
            {"keyId": "reviewer-key-a", "publicKeyBase64": encoded, "status": "active"},
            {"keyId": "reviewer-key-b", "publicKeyBase64": encoded, "status": "active"},
        ],
        "baselineKeys": [],
        "ledgerKeys": [],
    }
    path = tmp_path / "keyring.json"
    path.write_text(__import__("json").dumps(keyring), encoding="utf-8")
    try:
        monitor.load_keyring(path)
    except ValueError as exc:
        assert "distinct" in str(exc)
    else:
        raise AssertionError("two reviewer identities must not share one signing key")

    cross_role = {
        **keyring,
        "keyringVersion": "cross-role-keyring-v1",
        "reviewerKeys": [{"keyId": "reviewer-key-a", "publicKeyBase64": encoded, "status": "active"}],
        "ledgerKeys": [{"keyId": "score-ledger-key-a", "publicKeyBase64": encoded, "status": "active"}],
    }
    path.write_text(__import__("json").dumps(cross_role), encoding="utf-8")
    try:
        monitor.load_keyring(path)
    except ValueError as exc:
        assert "separated" in str(exc)
    else:
        raise AssertionError("reviewer and score-ledger roles must not share signing material")


def test_bundled_policy_binds_monitor_schemas_and_keyring_by_digest() -> None:
    root = Path(__file__).parents[3]
    policy = __import__("json").loads((root / "config" / "product_metric_policy.json").read_text(encoding="utf-8"))
    monitoring = policy["acceptanceMonitoring"]
    manual = policy["manualEvaluation"]
    assert monitoring["monitorDigest"] == "sha256:" + hashlib.sha256(MODULE_PATH.read_bytes()).hexdigest()
    assert manual["manualSchemaDigest"] == "sha256:" + hashlib.sha256(
        (root / "config" / "manual_product_evaluation.schema.json").read_bytes()
    ).hexdigest()
    assert manual["preregistrationSchemaDigest"] == "sha256:" + hashlib.sha256(
        (root / "config" / "manual_product_preregistration.schema.json").read_bytes()
    ).hexdigest()
    keyring_path = root / "config" / "acceptance_monitor_public_keys.json"
    _, _, _, _, identity = monitor.load_keyring(keyring_path)
    assert monitoring["keyringVersion"] == identity["keyringVersion"]
    assert monitoring["keyringDigest"] == identity["keyringDigest"]
    assert monitoring["keyringFrozenAt"] == identity["keyringFrozenAt"]


def test_jsonl_evidence_is_hash_chained_and_tampering_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "monitor.jsonl"
    first = sample(datetime(2026, 7, 1, tzinfo=UTC))
    second = sample(datetime(2026, 7, 1, 0, 15, tzinfo=UTC))
    monitor.append_sample(path, first)
    monitor.append_sample(path, second)
    loaded = monitor.load_samples(path)
    assert len(loaded) == 2
    assert loaded[1]["previousHash"] == loaded[0]["sampleHash"]

    path.write_text(path.read_text(encoding="utf-8").replace('"sampleHealthy":true', '"sampleHealthy":false', 1), encoding="utf-8")
    try:
        monitor.load_samples(path)
    except ValueError as exc:
        assert "hash mismatch" in str(exc)
    else:
        raise AssertionError("tampered evidence must not load")
