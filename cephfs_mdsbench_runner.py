#!/usr/bin/env python3
"""CLI entrypoint for the cephfs-mdsbench benchmark.

Matrix orchestrator: BenchRunner. Control-plane workload: MdsbenchWorkloadRunner.
Remote driver: lib/workload/run_mdsbench_workload.py.
"""
import os
import sys

# Add project root to sys.path to allow importing lib and cephfs_perf_lib
# when running the script directly
project_root = os.path.abspath(os.path.dirname(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from lib.runner.cephfs_benchmark_runner import BenchRunner
from lib.mount.mount_manager import StubMountManager
from lib.workload.mdsbench_runner import MdsBenchWorkloadRunner


class MdsBenchRunner(BenchRunner):
    def get_workload_runner(self, executor, config, fs_names):
        return MdsBenchWorkloadRunner(executor, config, fs_names)

    def get_mount_and_ganesha(self, executor, config, cephfs_manager):
        # libcephfs only — no kernel/FUSE mounts required.
        return StubMountManager(executor, config, cephfs_manager), None


def main():
    runner = MdsBenchRunner(description="CephFS MDS-Bench SpecStorage-style Performance Runner")
    runner.run()


if __name__ == "__main__":
    main()
