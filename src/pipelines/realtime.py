"""Procesamiento en tiempo real por micro-lotes.

DolarAPI no publica un websocket: el patrón correcto es un productor que
consulta la API en intervalos cortos y un consumidor que transforma y
persiste. Solo se escriben hechos nuevos (hash de contenido distinto).
"""

from __future__ import annotations

import logging
import time
from queue import Empty, Queue
from threading import Event, Thread

from src.clients.dolar_api import DolarApiClient, RawQuote
from src.config import Settings, get_settings
from src.storage.parquet_store import ParquetLake
from src.transforms.bronze import build_bronze_frame
from src.transforms.gold import build_gold_tables
from src.transforms.silver import bronze_to_silver

logger = logging.getLogger(__name__)


def run_stream(
    interval_seconds: int | None = None,
    cycles: int | None = None,
    settings: Settings | None = None,
) -> dict[str, int]:
    """Levanta productor/consumidor hasta Ctrl+C o completar ``cycles``.

    Args:
        interval_seconds: Espera entre consultas. Por defecto, settings.
        cycles: Si se indica, corre N micro-lotes y termina (útil para demo).
        settings: Configuración opcional.

    Returns:
        Estado del lake al finalizar.
    """
    settings = settings or get_settings()
    interval = interval_seconds or settings.stream_interval_seconds
    lake = ParquetLake(settings)
    client = DolarApiClient(settings)

    queue: Queue[list[RawQuote] | None] = Queue(maxsize=4)
    stop = Event()

    producer = Thread(
        target=_produce,
        args=(client, queue, stop, interval, cycles),
        name="dolar-producer",
        daemon=True,
    )
    consumer = Thread(
        target=_consume,
        args=(lake, queue, stop),
        name="dolar-consumer",
        daemon=True,
    )

    logger.info(
        "Stream iniciado | intervalo=%ss | ciclos=%s",
        interval,
        cycles if cycles is not None else "infinito",
    )
    producer.start()
    consumer.start()

    try:
        producer.join()
        queue.put(None)
        consumer.join()
    except KeyboardInterrupt:
        logger.info("Señal de corte recibida, cerrando stream...")
        stop.set()
        queue.put(None)
        producer.join(timeout=5)
        consumer.join(timeout=5)

    status = lake.status()
    logger.info("Stream finalizado | %s", status)
    return status


def _produce(
    client: DolarApiClient,
    queue: Queue,
    stop: Event,
    interval: int,
    cycles: int | None,
) -> None:
    completed = 0
    while not stop.is_set():
        try:
            quotes = client.fetch_all()
            queue.put(quotes)
            completed += 1
            logger.info("Productor: lote %s encolado (%s registros)", completed, len(quotes))
        except Exception:
            logger.exception("Productor: error al consultar la API")

        if cycles is not None and completed >= cycles:
            break
        if stop.wait(interval):
            break


def _consume(lake: ParquetLake, queue: Queue, stop: Event) -> None:
    known_hashes = set()
    existing = lake.read_silver()
    if not existing.empty:
        known_hashes.update(existing["record_hash"].tolist())

    while not stop.is_set():
        try:
            item = queue.get(timeout=1)
        except Empty:
            continue

        if item is None:
            queue.task_done()
            break

        try:
            _process_microbatch(lake, item, known_hashes)
        except Exception:
            logger.exception("Consumidor: error al procesar micro-lote")
        finally:
            queue.task_done()


def _process_microbatch(
    lake: ParquetLake,
    quotes: list[RawQuote],
    known_hashes: set[str],
) -> None:
    bronze_df = build_bronze_frame(quotes)
    lake.append_bronze(bronze_df)

    silver_df = bronze_to_silver(bronze_df)
    new_rows = silver_df.loc[~silver_df["record_hash"].isin(known_hashes)]
    if new_rows.empty:
        logger.info("Consumidor: sin cambios de cotización, se omite Silver/Gold")
        return

    lake.merge_silver(new_rows)
    known_hashes.update(new_rows["record_hash"].tolist())
    gold_tables = build_gold_tables(lake.read_silver())
    lake.write_gold(gold_tables)
    logger.info("Consumidor: %s cotizaciones nuevas promovidas a Silver/Gold", len(new_rows))
