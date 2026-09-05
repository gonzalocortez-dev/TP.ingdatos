"""Transformaciones reutilizables (estilo vectorizado, sin loops de filas)."""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


def fill_null_values(
    df: pd.DataFrame, column_name: str, fill_value: Any
) -> pd.DataFrame:
    """Rellena nulos de una columna con un valor explícito.

    Args:
        df: DataFrame de trabajo.
        column_name: Columna a completar.
        fill_value: Valor de reemplazo.

    Returns:
        El mismo DataFrame, con la columna actualizada.
    """
    if column_name not in df.columns:
        logger.warning("Columna %s inexistente; se omite el fillna", column_name)
        return df
    df[column_name] = df[column_name].fillna(fill_value)
    return df


def explode_column(
    df_origin: pd.DataFrame,
    cols_to_select: list[str],
    col_to_explode: str,
    delimiter: str = ",",
) -> pd.DataFrame:
    """Expande una columna con valores delimitados a una fila por valor.

    Args:
        df_origin: DataFrame original.
        cols_to_select: Columnas a conservar.
        col_to_explode: Columna con valores separados por ``delimiter``.
        delimiter: Separador, por defecto coma.

    Returns:
        DataFrame largo con la columna explotada y valores recortados.
    """
    missing = [col for col in cols_to_select if col not in df_origin.columns]
    if missing:
        raise KeyError(f"Columnas no encontradas: {missing}")

    result = df_origin.loc[:, cols_to_select].copy()
    result[col_to_explode] = (
        result[col_to_explode].fillna("").astype(str).str.split(delimiter)
    )
    result = result.explode(col_to_explode, ignore_index=True)
    result[col_to_explode] = result[col_to_explode].str.strip()
    return result[result[col_to_explode] != ""].reset_index(drop=True)


def hash_payload(payload: dict[str, Any] | str) -> str:
    """Genera un hash estable del payload crudo para detectar repeticiones."""
    if isinstance(payload, dict):
        text = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    else:
        text = payload
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def business_key_series(df: pd.DataFrame) -> pd.Series:
    """Clave de negocio: source + casa + moneda + timestamp de la cotización."""
    return (
        df["source"].astype(str)
        + "|"
        + df["casa"].astype(str)
        + "|"
        + df["moneda"].astype(str)
        + "|"
        + df["fecha_actualizacion"].astype(str)
    )


def content_hash_series(df: pd.DataFrame) -> pd.Series:
    """Hash del contenido de negocio (ignora metadatos de ingestión)."""
    content = (
        df["source"].astype(str)
        + "|"
        + df["casa"].astype(str)
        + "|"
        + df["moneda"].astype(str)
        + "|"
        + df["compra"].astype(str)
        + "|"
        + df["venta"].astype(str)
        + "|"
        + df["fecha_actualizacion"].astype(str)
    )
    return content.map(lambda value: hashlib.sha256(value.encode("utf-8")).hexdigest())
