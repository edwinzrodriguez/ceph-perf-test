#!/usr/bin/env python3
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
from lib.workload.elbencho_runner import ElbenchoWorkloadRunner


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
        print(
            f"Using StubMountManager + RGW "
            f"(elbencho runner, type={config.rgw_type})"
        )
        return StubMountManager(executor, config, cephfs_manager), export_manager


def main():
    runner = ElbenchoBenchRunner(description="Elbencho RGW/S3 Performance Runner")
    runner.run()


if __name__ == "__main__":
    main()
