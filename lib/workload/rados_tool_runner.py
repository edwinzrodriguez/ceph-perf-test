"""Deprecated import path. Use ``lib.workload.rados_bench_runner``."""

from lib.workload.rados_bench_runner import RadosBenchWorkloadRunner
from lib.workload.rados_bench_runner import (
    RadosBenchWorkloadRunner as RadosToolWorkloadRunner,
)

__all__ = ["RadosBenchWorkloadRunner", "RadosToolWorkloadRunner"]
