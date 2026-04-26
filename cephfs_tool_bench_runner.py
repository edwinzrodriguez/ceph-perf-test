#!/usr/bin/env python3
import os
import sys

# Add project root to sys.path to allow importing lib and cephfs_perf_lib
# when running the script directly
project_root = os.path.abspath(os.path.dirname(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

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
