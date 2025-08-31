"""
Data Pipeline Package

A collection of utilities for concurrent data processing in Python.

This package provides:
- AsyncDataFetcher: For asynchronous HTTP requests with concurrency control
- CPUIntensiveProcessor: For parallel processing of CPU-bound tasks

Version: 0.1.0
"""

from .async_io import AsyncDataFetcher
from .cpu_process import CPUIntensiveProcessor, is_prime

__version__ = "0.1.0"
__all__ = ["AsyncDataFetcher", "CPUIntensiveProcessor", "is_prime"]
