#!/usr/bin/env python3
from lib.runner.cephfs_benchmark_runner import BenchRunner
from lib.workload.fio_runner import FioWorkloadRunner
from lib.workload.cephfs_tool_runner import CephFSToolWorkloadRunner
from lib.workload.spec_storage_runner import SpecStorageWorkloadRunner


class MainBenchRunner(BenchRunner):
    def get_workload_runner(self, executor, config, fs_names):
        if config.fio:
            return FioWorkloadRunner(executor, config, fs_names)
        elif config.cephfs_tool:
            return CephFSToolWorkloadRunner(executor, config, fs_names)
        else:
            return SpecStorageWorkloadRunner(executor, config, fs_names)


def main():
    runner = MainBenchRunner(description="CephFS Performance Runner")
    runner.run()


if __name__ == "__main__":
    main()
