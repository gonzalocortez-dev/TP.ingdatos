"""Capa Silver: limpieza, tipado, métricas y prevención de duplicados.

Cada función es una transformación de datos independiente y vectorizada.
El orquestador ``bronze_to_silver`` las aplica en un orden determinista.
"""

from __future__ import annotations

import json
import logging

import pandas as pd

from src.transforms.common import (
    business_key_series,
    content_hash_series,
    fill_null_values,
)

logger = logging.getLogger(__name__)

# Clasificación de mercados para explotar luego en Gold (bridge de tags).
MARKET_TAGS: dict[str, str] = {
    "oficial": "regulado,banco,bcra",
    "blue": "paralelo,informal",
    "bolsa": "financiero,merval,mep",
    "contadoconliqui": "financiero,ccl,bursatil",
    "mayorista": "regulado,interbancario",
    "cripto": "digital,crypto",
    "tarjeta": "regulado,impuesto,turismo",
}

STRING_FILL_MAP: dict[str, str] = {
    "casa": "desconocida",
    "nombre": "N/A",
    "moneda": "N/A",
}

SILVER_EMPTY_COLUMNS = [
    "ingest_id",
    "ingest_ts",
    "source",
    "endpoint",
    "moneda",
    "casa",
    "nombre",
    "compra",
    "venta",
    "fecha_actualizacion",
    "fecha_actualizacion_iso",
    "market_tags",
    "spread",
    "mid_price",
    "spread_pct",
    "quality_ok",
    "quality_flags",
    "business_key",
    "record_hash",
    "year",
    "month",
    "day",
]


def parse_bronze_payload(bronze_df: pd.DataFrame) -> pd.DataFrame:
    """T1 — Extrae el JSON crudo a columnas de negocio."""
    if bronze_df.empty:
        return pd.DataFrame(columns=SILVER_EMPTY_COLUMNS)

    parsed = bronze_df["payload_json"].map(json.loads)
    parsed_df = pd.json_normalize(parsed.tolist())
    frame = pd.concat(
        [bronze_df[["ingest_id", "ingest_ts", "source", "endpoint"]].reset_index(drop=True), parsed_df],
        axis=1,
    )
    return frame


def normalize_schema(df: pd.DataFrame) -> pd.DataFrame:
    """T2 — Homogeneiza nombres, recorta texto y asegura columnas esperadas."""
    rename_map = {"fechaActualizacion": "fecha_actualizacion"}
    df = df.rename(columns=rename_map)

    expected = ["moneda", "casa", "nombre", "compra", "venta", "fecha_actualizacion"]
    for column in expected:
        if column not in df.columns:
            df[column] = pd.NA

    for column in ("moneda", "casa", "nombre"):
        df[column] = df[column].astype("string").str.strip().str.lower()
        if column == "nombre":
            df[column] = df[column].str.title()
        if column == "moneda":
            df[column] = df[column].str.upper()

    return df


def fill_missing_attributes(df: pd.DataFrame) -> pd.DataFrame:
    """T3 — Completa nulos categóricos con valores de negocio explícitos."""
    for column, value in STRING_FILL_MAP.items():
        df = fill_null_values(df, column, value)
    return df


def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    """T4 — Convierte precios a numérico y timestamps a UTC."""
    df["compra"] = pd.to_numeric(df["compra"], errors="coerce")
    df["venta"] = pd.to_numeric(df["venta"], errors="coerce")
    df["fecha_actualizacion"] = pd.to_datetime(
        df["fecha_actualizacion"], utc=True, errors="coerce"
    )
    df["ingest_ts"] = pd.to_datetime(df["ingest_ts"], utc=True, errors="coerce")

    missing_ts = df["fecha_actualizacion"].isna()
    df.loc[missing_ts, "fecha_actualizacion"] = pd.Timestamp("1900-01-01", tz="UTC")
    df["fecha_actualizacion_iso"] = df["fecha_actualizacion"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return df


def add_partition_columns(df: pd.DataFrame) -> pd.DataFrame:
    """T5 — Agrega columnas de partición a partir de la fecha de cotización."""
    df["year"] = df["fecha_actualizacion"].dt.year.astype("int32")
    df["month"] = df["fecha_actualizacion"].dt.month.astype("int32")
    df["day"] = df["fecha_actualizacion"].dt.day.astype("int32")
    return df


def assign_market_tags(df: pd.DataFrame) -> pd.DataFrame:
    """T6 — Asigna tags de mercado (luego se explotan en Gold)."""
    df["market_tags"] = df["casa"].map(MARKET_TAGS).fillna("otro")
    return df


def compute_price_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """T7 — Calcula spread, precio medio y spread porcentual (vectorizado)."""
    df["spread"] = df["venta"] - df["compra"]
    df["mid_price"] = (df["venta"] + df["compra"]) / 2.0
    df["spread_pct"] = (df["spread"] / df["compra"]) * 100.0
    df.loc[df["compra"].fillna(0) <= 0, "spread_pct"] = pd.NA
    return df


def apply_quality_rules(df: pd.DataFrame) -> pd.DataFrame:
    """T8 — Marca calidad sin borrar filas (trazabilidad completa en Silver)."""
    flag_map = {
        "precio_nulo": df["compra"].isna() | df["venta"].isna(),
        "compra_negativa": df["compra"].fillna(-1) < 0,
        "venta_negativa": df["venta"].fillna(-1) < 0,
        "venta_menor_compra": df["venta"] < df["compra"],
        "fecha_default": df["fecha_actualizacion"] == pd.Timestamp("1900-01-01", tz="UTC"),
    }
    quality_matrix = pd.DataFrame(flag_map)
    df["quality_ok"] = ~quality_matrix.any(axis=1)
    df["quality_flags"] = ""
    for name, mask in flag_map.items():
        df.loc[mask, "quality_flags"] = df.loc[mask, "quality_flags"] + name + ","
    df["quality_flags"] = df["quality_flags"].str.rstrip(",")
    return df


def add_identity_keys(df: pd.DataFrame) -> pd.DataFrame:
    """T9 — Clave de negocio + hash de contenido para upsert/dedup."""
    df["business_key"] = business_key_series(df)
    df["record_hash"] = content_hash_series(df)
    return df


def deduplicate_quotes(df: pd.DataFrame) -> pd.DataFrame:
    """T10 — Elimina repeticiones conservando la ingestión más reciente.

    Estrategia:
    1. Misma ``business_key`` y mismo ``record_hash`` → es el mismo hecho, se descarta.
    2. Misma ``business_key`` y hash distinto → se queda la fila con mayor ``ingest_ts``.
    """
    if df.empty:
        return df

    ordered = df.sort_values(
        ["business_key", "ingest_ts"], ascending=[True, False], kind="mergesort"
    )
    deduped = ordered.drop_duplicates(subset=["business_key"], keep="first")
    removed = len(df) - len(deduped)
    if removed:
        logger.info("Deduplicación Silver: se removieron %s filas repetidas", removed)
    return deduped.reset_index(drop=True)


def bronze_to_silver(bronze_df: pd.DataFrame) -> pd.DataFrame:
    """Aplica en orden las transformaciones Silver.

    Returns:
        DataFrame Silver listo para merge particionado en el lake.
    """
    if bronze_df.empty:
        return pd.DataFrame(columns=SILVER_EMPTY_COLUMNS)

    df = parse_bronze_payload(bronze_df)
    df = normalize_schema(df)
    df = fill_missing_attributes(df)
    df = cast_types(df)
    df = add_partition_columns(df)
    df = assign_market_tags(df)
    df = compute_price_metrics(df)
    df = apply_quality_rules(df)
    df = add_identity_keys(df)
    df = deduplicate_quotes(df)
    return df.loc[:, SILVER_EMPTY_COLUMNS]
