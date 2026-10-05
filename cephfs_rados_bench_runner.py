#!/usr/bin/env python3
"""CLI entrypoint for the rados bench benchmark.

Matrix orchestrator: BenchRunner. Control-plane workload: RadosBenchWorkloadRunner.
Remote driver: lib/workload/run_rados_workload.py.
"""
import os
import sys

# Add project root to sys.path to allow importing lib and cephfs_perf_lib
# when running the script directly
project_root = os.path.abspath(os.path.dirname(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from lib.runner.cephfs_benchmark_runner import BenchRunner
from lib.fs.ceph_pool_manager import CephPoolManager
from lib.mount.mount_manager import StubMountManager
from lib.workload.rados_bench_runner import RadosBenchWorkloadRunner


class RadosBenchRunner(BenchRunner):
    def get_workload_runner(self, executor, config, fs_names):
        return RadosBenchWorkloadRunner(executor, config, fs_names)

    def get_fs_manager(self, executor, config):
        # Rados bench doesn't need a CephFS — just a pool. Force the pool
        # manager regardless of the yaml's fs_manager_type setting so a
        # shared config file used by multiple runners still Does The Right
        # Thing when invoked as the rados runner.
        return CephPoolManager(executor, config, section="rados_bench")

    def get_mount_and_ganesha(self, executor, config, cephfs_manager):
        # No mount, no NFS. Rados bench talks to OSDs directly.
        return StubMountManager(executor, config, cephfs_manager), None


def main():
    runner = RadosBenchRunner(description="Rados Bench Performance Runner")
    runner.run()


if __name__ == "__main__":
    main()
