#!/usr/bin/env python3
from lib.runner.cephfs_benchmark_runner import BenchRunner
from lib.workload.fio_runner import FioWorkloadRunner

class FioBenchRunner(BenchRunner):
    def get_workload_runner(self, executor, config, fs_names):
        return FioWorkloadRunner(executor, config, fs_names)

def main():
    runner = FioBenchRunner(description="CephFS fio Performance Runner")
    runner.run()

if __name__ == "__main__":
    main()
