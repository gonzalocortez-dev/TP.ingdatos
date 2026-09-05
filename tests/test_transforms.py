"""Pruebas de las transformaciones Silver y Gold."""

from __future__ import annotations

import pandas as pd

from src.transforms.common import explode_column, fill_null_values
from src.transforms.gold import build_gold_tables
from src.transforms.silver import bronze_to_silver, deduplicate_quotes


def test_fill_null_values_replaces_nans() -> None:
    frame = pd.DataFrame({"casa": ["blue", None]})
    result = fill_null_values(frame, "casa", "N/A")
    assert result["casa"].tolist() == ["blue", "N/A"]


def test_explode_column_creates_one_row_per_tag() -> None:
    frame = pd.DataFrame(
        {
            "casa": ["oficial"],
            "market_tags": ["regulado, banco, bcra"],
        }
    )
    exploded = explode_column(frame, ["casa", "market_tags"], "market_tags")
    assert exploded["market_tags"].tolist() == ["regulado", "banco", "bcra"]


def test_silver_applies_core_transformations(bronze_df) -> None:
    silver = bronze_to_silver(bronze_df)

    assert not silver.empty
    assert silver["casa"].tolist().count("oficial") >= 1
    assert silver["fecha_actualizacion_iso"].str.match(r"\d{4}-\d{2}-\d{2}").all()
    assert (silver["spread"] == silver["venta"] - silver["compra"]).all()
    assert silver["quality_ok"].all()
    assert {"year", "month", "day", "business_key", "record_hash"} <= set(silver.columns)


def test_deduplicate_keeps_single_business_key(duplicated_bronze) -> None:
    silver = bronze_to_silver(duplicated_bronze)
    assert silver["business_key"].is_unique
    again = pd.concat([silver, silver], ignore_index=True)
    assert len(deduplicate_quotes(again)) == len(silver)


def test_gold_builds_analytical_tables(bronze_df) -> None:
    silver = bronze_to_silver(bronze_df)
    gold = build_gold_tables(silver)

    assert set(gold) == {
        "latest",
        "daily_metrics",
        "brecha",
        "prices_long",
        "market_tags",
        "fx_cross",
        "variations",
    }
    latest = gold["latest"]
    assert latest.duplicated(subset=["casa", "moneda"]).sum() == 0
    assert "blue" in latest["casa"].tolist()
    assert not gold["market_tags"].empty
    assert {"compra", "venta"} <= set(gold["prices_long"]["tipo_precio"])
    assert "EUR" in gold["fx_cross"]["moneda"].tolist()
