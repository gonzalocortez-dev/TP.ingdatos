"""Pruebas del extractor incremental y del watermark persistido (sin Spark)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from bronze_layer import resolve_bronze_write_strategy
from extractors import DolarApiExtractor, _parse_api_timestamp


def test_parse_api_timestamp_zulu() -> None:
    parsed = _parse_api_timestamp("2026-09-05T20:56:00.000Z")
    assert parsed is not None
    assert parsed.tzinfo is not None


def test_incremental_filters_by_watermark(tmp_path: Path, monkeypatch) -> None:
    extractor = DolarApiExtractor(watermark_path=tmp_path / "wm.json")

    def fake_full():
        return [
            {
                "casa": "oficial",
                "fechaActualizacion": "2026-09-01T10:00:00.000Z",
                "_endpoint": "/v1/dolares",
            },
            {
                "casa": "blue",
                "fechaActualizacion": "2026-09-05T10:00:00.000Z",
                "_endpoint": "/v1/dolares",
            },
        ]

    monkeypatch.setattr(extractor, "extract_full", fake_full)
    extractor.save_watermark(
        [{"fechaActualizacion": "2026-09-03T00:00:00.000Z"}],
        mode="full",
    )
    rows = extractor.extract_incremental()
    assert len(rows) == 1
    assert rows[0]["casa"] == "blue"


def test_watermark_roundtrip(tmp_path: Path) -> None:
    extractor = DolarApiExtractor(watermark_path=tmp_path / "wm.json")
    extractor.save_watermark(
        [{"fechaActualizacion": "2026-09-05T20:56:00.000Z"}],
        mode="full",
    )
    loaded = extractor.load_watermark()
    assert loaded == datetime(2026, 9, 5, 20, 56, tzinfo=timezone.utc)

    state = json.loads((tmp_path / "wm.json").read_text(encoding="utf-8"))
    assert state["watermark_field"] == "fechaActualizacion"
    assert state["last_mode"] == "full"
    assert state["last_records"] == 1
    assert state["last_run_at"]


def test_empty_lote_keeps_previous_watermark(tmp_path: Path) -> None:
    extractor = DolarApiExtractor(watermark_path=tmp_path / "wm.json")
    extractor.save_watermark(
        [{"fechaActualizacion": "2026-09-05T20:56:00.000Z"}],
        mode="full",
    )
    extractor.save_watermark([], mode="incremental")
    assert extractor.load_watermark() == datetime(2026, 9, 5, 20, 56, tzinfo=timezone.utc)
    state = json.loads((tmp_path / "wm.json").read_text(encoding="utf-8"))
    assert state["last_mode"] == "incremental"
    assert state["last_records"] == 0


def test_bronze_write_strategy() -> None:
    assert resolve_bronze_write_strategy("full", True) == "overwrite"
    assert resolve_bronze_write_strategy("full", False) == "overwrite"
    assert resolve_bronze_write_strategy("incremental", True) == "merge"
    assert resolve_bronze_write_strategy("incremental", False) == "overwrite"
