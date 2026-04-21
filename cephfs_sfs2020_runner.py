#!/usr/bin/env python3
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
