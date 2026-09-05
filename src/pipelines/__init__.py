"""Pipelines batch y en tiempo real."""

from src.pipelines.batch import run_batch
from src.pipelines.realtime import run_stream

__all__ = ["run_batch", "run_stream"]
