"""Transformaciones PySpark: de Bronze (crudo) a Silver (limpio).

Se ejecutan DESPUÉS del Load. Es el paso T del ELT.

Qué se hace acá (y no en Bronze)
--------------------------------
1. Casteo de compra/venta a double y fechas a timestamp UTC.
2. Normalización de texto (trim, casing).
3. Imputación de nulos categóricos.
4. Deduplicación por clave de negocio.
5. Columnas de negocio: spread, mid_price, quality_ok, partition cols.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


def bronze_to_silver(bronze_df: DataFrame) -> DataFrame:
    """Aplica la cadena de limpieza sobre el histórico Bronze.

    Args:
        bronze_df: DataFrame leído de data_lake/bronze/quotes (Delta).

    Returns:
        DataFrame Silver listo para persistir.
    """
    df = _cast_and_normalize(bronze_df)
    df = _fill_nulls(df)
    df = _add_business_columns(df)
    df = _flag_quality(df)
    df = _deduplicate(df)
    logger.info("Transform | Bronze → Silver listo")
    return df


def _cast_and_normalize(df: DataFrame) -> DataFrame:
    """Tipos reales + nombres snake_case. Acá sí se interpreta el crudo."""
    return (
        df.select(
            F.col("origen_datos").alias("source"),
            F.col("origen_datos"),
            F.col("endpoint"),
            F.col("fecha_ingesta").alias("fecha_ingesta_raw"),
            F.col("modo_extraccion"),
            F.col("batch_id"),
            F.trim(F.col("moneda")).alias("moneda_raw"),
            F.trim(F.col("casa")).alias("casa_raw"),
            F.trim(F.col("nombre")).alias("nombre_raw"),
            F.col("compra").alias("compra_raw"),
            F.col("venta").alias("venta_raw"),
            F.col("fechaActualizacion").alias("fecha_raw"),
            F.col("raw_json"),
        )
        .withColumn("moneda", F.upper(F.col("moneda_raw")))
        .withColumn("casa", F.lower(F.col("casa_raw")))
        .withColumn("nombre", F.initcap(F.col("nombre_raw")))
        .withColumn("compra", F.col("compra_raw").cast("double"))
        .withColumn("venta", F.col("venta_raw").cast("double"))
        .withColumn(
            "fecha_actualizacion",
            F.to_timestamp(F.col("fecha_raw")),
        )
        .withColumn("fecha_ingesta", F.to_timestamp(F.col("fecha_ingesta_raw")))
        .withColumn("ingest_ts", F.col("fecha_ingesta"))
    )


def _fill_nulls(df: DataFrame) -> DataFrame:
    """Completa categóricos. Los precios nulos se dejan nulos y se marcan después."""
    return (
        df.fillna({"casa": "desconocida", "nombre": "N/A", "moneda": "N/A"})
        .withColumn(
            "fecha_actualizacion",
            F.coalesce(
                F.col("fecha_actualizacion"),
                F.to_timestamp(F.lit("1900-01-01 00:00:00")),
            ),
        )
    )


def _add_business_columns(df: DataFrame) -> DataFrame:
    """Métricas y particiones. Solo tienen sentido sobre datos ya tipados."""
    market_tags = (
        F.when(F.col("casa") == "oficial", F.lit("regulado,banco,bcra"))
        .when(F.col("casa") == "blue", F.lit("paralelo,informal"))
        .when(F.col("casa") == "bolsa", F.lit("financiero,merval,mep"))
        .when(F.col("casa") == "contadoconliqui", F.lit("financiero,ccl,bursatil"))
        .when(F.col("casa") == "mayorista", F.lit("regulado,interbancario"))
        .when(F.col("casa") == "cripto", F.lit("digital,crypto"))
        .when(F.col("casa") == "tarjeta", F.lit("regulado,impuesto,turismo"))
        .otherwise(F.lit("otro"))
    )
    return (
        df.withColumn("spread", F.col("venta") - F.col("compra"))
        .withColumn("mid_price", (F.col("venta") + F.col("compra")) / F.lit(2.0))
        .withColumn(
            "spread_pct",
            F.when(F.col("compra") > 0, (F.col("spread") / F.col("compra")) * 100.0),
        )
        .withColumn("market_tags", market_tags)
        .withColumn("year", F.year("fecha_actualizacion"))
        .withColumn("month", F.month("fecha_actualizacion"))
        .withColumn("day", F.dayofmonth("fecha_actualizacion"))
        .withColumn(
            "business_key",
            F.concat_ws(
                "|",
                F.col("source"),
                F.col("casa"),
                F.col("moneda"),
                F.col("fecha_actualizacion").cast("string"),
            ),
        )
    )


def _flag_quality(df: DataFrame) -> DataFrame:
    """Marca calidad sin borrar filas: Silver conserva trazabilidad."""
    invalid = (
        F.col("compra").isNull()
        | F.col("venta").isNull()
        | (F.col("compra") < 0)
        | (F.col("venta") < 0)
        | (F.col("venta") < F.col("compra"))
        | (F.col("fecha_actualizacion") == F.to_timestamp(F.lit("1900-01-01 00:00:00")))
    )
    return df.withColumn("quality_ok", ~invalid)


def _deduplicate(df: DataFrame) -> DataFrame:
    """Una fila vigente por business_key: gana la ingestión más reciente."""
    window = Window.partitionBy("business_key").orderBy(F.col("ingest_ts").desc_nulls_last())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def select_silver_columns(df: DataFrame) -> DataFrame:
    """Contrato de columnas de la tabla Silver."""
    return df.select(
        "business_key",
        "origen_datos",
        "source",
        "endpoint",
        "modo_extraccion",
        "batch_id",
        "moneda",
        "casa",
        "nombre",
        "compra",
        "venta",
        "spread",
        "mid_price",
        "spread_pct",
        "market_tags",
        "fecha_actualizacion",
        "fecha_ingesta",
        "ingest_ts",
        "quality_ok",
        "year",
        "month",
        "day",
        "raw_json",
    )
