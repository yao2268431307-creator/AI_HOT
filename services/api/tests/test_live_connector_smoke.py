from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path

import pytest


MODULE_PATH = Path(__file__).parents[3] / "tools" / "live_connector_smoke.py"
SPEC = importlib.util.spec_from_file_location("live_connector_smoke_cli", MODULE_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


@pytest.mark.parametrize("value, expected", [("1", 1), ("3", 3), ("10", 10)])
def test_hn_smoke_item_limit_accepts_only_bounded_values(value: str, expected: int) -> None:
    assert MODULE.bounded_hn_items(value) == expected


@pytest.mark.parametrize("value", ["-1", "0", "11", "all"])
def test_hn_smoke_item_limit_rejects_unbounded_values(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        MODULE.bounded_hn_items(value)


@pytest.mark.asyncio
async def test_live_smoke_does_not_report_empty_mapping_as_pass() -> None:
    class EmptyConnector:
        id = "empty"
        closed = False

        async def collect(self) -> list[object]:
            return []

        def drain_request_count(self) -> int:
            return 1

        async def close(self) -> None:
            self.closed = True

    connector = EmptyConnector()
    result = await MODULE.probe(connector)
    assert result["status"] == "fail"
    assert result["observations"] == 0
    assert result["error"] == "RuntimeError: connector returned zero observations"
    assert connector.closed is True
