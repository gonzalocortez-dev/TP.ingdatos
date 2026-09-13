"""Orquestador ELT: Extract → Load (Bronze) → Transform (Silver) → Gold.

Uso
---
    python main.py --mode full
    python main.py --mode incremental

Por qué este orden
------------------
1. EXTRACT: se habla con la API. Todavía no hay Spark ni limpieza.
2. LOAD Bronze: se persiste el crudo en Delta. Si el transform falla después,
   no se pierde la extracción (se puede reprocesar Silver desde Bronze).
3. TRANSFORM Silver: se lee Bronze con PySpark y se limpia.
4. GOLD: se agregan métricas OLAP sobre Silver válido.

Cada capa vive en su carpeta Delta. Mover datos de una a otra cambia el
propósito: crudo → hecho de negocio → modelo analítico.
"""

from __future__ import annotations

import argparse
import logging
import sys

from bronze_layer import load_bronze, read_bronze
from config import LAKE_ROOT, ensure_lake_dirs
from extractors import DolarApiExtractor, ExtractMode
from gold_layer import build_and_persist_gold, persist_silver
from spark_session import get_spark
from transform_layer import transform_bronze_to_silver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("elt")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline ELT medallion (DolarAPI → Delta Lake Bronze/Silver/Gold)."
    )
    parser.add_argument(
        "--mode",
        choices=("full", "incremental"),
        default="full",
        help="full: snapshot + overwrite Bronze. incremental: watermark + MERGE.",
    )
    return parser.parse_args()


def run_pipeline(mode: ExtractMode) -> dict[str, int]:
    """Ejecuta las cuatro etapas y devuelve conteos por capa."""
    ensure_lake_dirs()
    extractor = DolarApiExtractor()

    # --- 1. EXTRACT -------------------------------------------------------
    logger.info("=== EXTRACT (%s) ===", mode)
    raw_records = extractor.extract(mode)
    if not raw_records:
        extractor.save_watermark(raw_records, mode=mode)
        logger.info("Sin registros nuevos. Se corta el pipeline (nada que cargar).")
        return {"extracted": 0, "bronze_written": 0}

    # --- 2. LOAD Bronze (crudo, Delta: full=overwrite / incremental=MERGE) --
    logger.info("=== LOAD Bronze (Delta, sin transformaciones de negocio) ===")
    spark = get_spark()
    bronze_written = load_bronze(spark, raw_records, ingest_mode=mode)
    extractor.save_watermark(raw_records, mode=mode)

    # --- 3. TRANSFORM + persist Silver ------------------------------------
    logger.info("=== TRANSFORM Bronze → Silver ===")
    bronze_df = read_bronze(spark)
    silver_df = transform_bronze_to_silver(bronze_df)
    persist_silver(silver_df)

    # --- 4. GOLD (agregados OLAP) -----------------------------------------
    logger.info("=== GOLD (agregaciones analíticas) ===")
    gold_counts = build_and_persist_gold(silver_df)

    status = {
        "extracted": len(raw_records),
        "bronze_written": bronze_written,
        "silver": silver_df.count(),
        **gold_counts,
    }
    logger.info("Pipeline OK | lake=%s | %s", LAKE_ROOT.resolve(), status)
    return status


def main() -> None:
    args = parse_args()
    try:
        run_pipeline(args.mode)
    except Exception:
        logger.exception("El pipeline falló.")
        sys.exit(1)


if __name__ == "__main__":
    main()
