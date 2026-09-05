"""Configuración de logging para una ejecución clara y trazable."""

from __future__ import annotations

import logging


def setup_logging(level: str = "INFO") -> None:
    """Inicializa el logging raíz una sola vez.

    Args:
        level: Nivel de logging (DEBUG, INFO, WARNING, ERROR).
    """
    root = logging.getLogger()
    if root.handlers:
        root.setLevel(level)
        return

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
