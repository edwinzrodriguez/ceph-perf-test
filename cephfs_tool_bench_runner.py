#!/usr/bin/env python3
from lib.runner.cephfs_benchmark_runner import BenchRunner
from lib.workload.cephfs_tool_runner import CephFSToolWorkloadRunner


class ToolBenchRunner(BenchRunner):
    def get_workload_runner(self, executor, config, fs_names):
        return CephFSToolWorkloadRunner(executor, config, fs_names)


def main():
    runner = ToolBenchRunner(description="CephFS Tool Bench Performance Runner")
    runner.run()


if __name__ == "__main__":
    main()
