"""Punto de entrada único del proyecto.

Ejemplos::

    python -m src.cli batch
    python -m src.cli stream --interval 20 --cycles 3
    python -m src.cli status
"""

from __future__ import annotations

import argparse
import logging

from src.config import get_settings
from src.logging_config import setup_logging
from src.pipelines.batch import run_batch
from src.pipelines.realtime import run_stream
from src.storage.parquet_store import ParquetLake

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Pipeline medallion de cotizaciones (DolarAPI → Parquet)."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("batch", help="Una corrida completa Bronze → Silver → Gold")

    stream = subparsers.add_parser(
        "stream", help="Micro-lotes en tiempo real (productor/consumidor)"
    )
    stream.add_argument(
        "--interval",
        type=int,
        default=None,
        help="Segundos entre consultas a la API (default: STREAM_INTERVAL_SECONDS)",
    )
    stream.add_argument(
        "--cycles",
        type=int,
        default=None,
        help="Cantidad de micro-lotes. Sin valor, corre hasta Ctrl+C",
    )

    subparsers.add_parser("status", help="Muestra filas persistidas en el lake")
    return parser


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    args = build_parser().parse_args()

    if args.command == "batch":
        status = run_batch(settings)
        _print_status(status)
        return

    if args.command == "stream":
        status = run_stream(
            interval_seconds=args.interval,
            cycles=args.cycles,
            settings=settings,
        )
        _print_status(status)
        return

    lake = ParquetLake(settings)
    _print_status(lake.status())


def _print_status(status: dict[str, int]) -> None:
    logger.info("Estado del lake:")
    for name, count in status.items():
        logger.info("  %-20s %s", name, count)


if __name__ == "__main__":
    main()
