#!/usr/bin/env python3
"""CLI entrypoint for the SFS2020 / SPEC Storage benchmark.

Matrix orchestrator: BenchRunner. Control-plane workload: SpecStorageWorkloadRunner.
Remote driver: lib/workload/run_sfs2020_workload.py.
"""
import os
import sys

# Add project root to sys.path to allow importing lib and cephfs_perf_lib
# when running the script directly
project_root = os.path.abspath(os.path.dirname(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from lib.runner.cephfs_benchmark_runner import BenchRunner
from lib.workload.spec_storage_runner import SpecStorageWorkloadRunner


class SfsBenchRunner(BenchRunner):
    def get_workload_runner(self, executor, config, fs_names):
        return SpecStorageWorkloadRunner(executor, config, fs_names)


def main():
    runner = SfsBenchRunner(description="CephFS SpecStorage2020 Performance Runner")
    runner.run()


if __name__ == "__main__":
    main()
