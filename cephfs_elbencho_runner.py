#!/usr/bin/env python3
"""CLI entrypoint for the elbencho RGW benchmark.

Matrix orchestrator: BenchRunner. Control-plane workload: ElbenchoWorkloadRunner.
Remote driver: lib/workload/run_elbencho_workload.py.
"""
import os
import sys

project_root = os.path.abspath(os.path.dirname(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from cephfs_perf_lib import StubFSManager
from lib.mount.mount_manager import StubMountManager
from lib.rgw.rgw_cephadm_manager import RgwCephadmManager
from lib.rgw.rgw_systemd_manager import RgwSystemdManager
from lib.runner.cephfs_benchmark_runner import BenchRunner
from lib.run_logging import get_logger
from lib.workload.elbencho_runner import ElbenchoWorkloadRunner

log = get_logger(__name__)


class ElbenchoBenchRunner(BenchRunner):
    """RGW + elbencho S3 benchmark entry point.

    Forces StubFSManager (no CephFS) and always provisions RGW for the
    inventory ``rgws`` group. Enable type via ``rgw.type`` or ``--rgw``.
    """

    def get_workload_runner(self, executor, config, fs_names):
        return ElbenchoWorkloadRunner(executor, config, fs_names)

    def get_fs_manager(self, executor, config):
        return StubFSManager(config)

    def get_mount_and_ganesha(self, executor, config, cephfs_manager):
        # Ensure RGW is enabled for this runner even if the shared YAML left
        # rgw.enabled false (same idea as rados forcing StubMount).
        config._config.setdefault("rgw", {})["enabled"] = True
        if config.rgw_type == "systemd":
            export_manager = RgwSystemdManager(executor, config, cephfs_manager)
        elif config.rgw_type == "cephadm":
            export_manager = RgwCephadmManager(executor, config, cephfs_manager)
        else:
            raise ValueError(f"Invalid RGW type: {config.rgw_type}")
        log.info(
            "Using StubMountManager + RGW (elbencho runner, type=%s)",
            config.rgw_type,
        )
        return StubMountManager(executor, config, cephfs_manager), export_manager


def main():
    runner = ElbenchoBenchRunner(description="Elbencho RGW/S3 Performance Runner")
    runner.run()


if __name__ == "__main__":
    main()
