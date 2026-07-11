#!/usr/bin/env python3
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
from lib.workload.rbd_runner import RbdWorkloadRunner


class RbdBenchRunner(BenchRunner):
    def get_workload_runner(self, executor, config, fs_names):
        return RbdWorkloadRunner(executor, config, fs_names)

    def get_fs_manager(self, executor, config):
        # RBD uses librbd directly — no CephFS, just a pool.
        return CephPoolManager(executor, config, section="rbd")

    def get_mount_and_ganesha(self, executor, config, cephfs_manager):
        # No mount, no ganesha. fio's rbd ioengine talks to OSDs directly.
        return StubMountManager(executor, config, cephfs_manager), None


def main():
    runner = RbdBenchRunner(description="RBD (fio --ioengine=rbd) Performance Runner")
    runner.run()


if __name__ == "__main__":
    main()
