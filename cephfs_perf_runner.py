#!/usr/bin/env python3
import sys
import yaml
import datetime
import itertools
from cephfs_perf_lib import (
    AnsibleInventoryProvider,
    PerformanceTestConfig,
    SSHExecutor,
    CephFSManager,
    GaneshaManager,
    MountManager,
    WorkloadRunner,
    SpecStorageWorkloadRunner,
    CommonUtils,
)


def main():
    if len(sys.argv) < 3:
        print("Usage: cephfs-perf-runner <config.yaml> <ansible_inventory>")
        sys.exit(1)

    config_path = sys.argv[1]
    inventory_path = sys.argv[2]

    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)

    inventory_provider = AnsibleInventoryProvider(inventory_path)
    config = PerformanceTestConfig(config_dict, inventory_provider)

    executor = SSHExecutor(config.all_hosts_meta)
    cephfs_manager = CephFSManager(executor, config)
    ganesha_manager = GaneshaManager(executor, config)
    mount_manager = MountManager(executor, config)
    workload_runner = SpecStorageWorkloadRunner(executor, config, cephfs_manager.fs_names)

    # Execute test matrix
    mount_manager.unmount_clients()

    now = datetime.datetime.now(datetime.timezone.utc)
    timestamp = now.strftime("%Y%m%d-%H%M%S")
    unix_ts = int(now.timestamp())
    shared_timestamp = f"{timestamp}-{unix_ts}"

    keys = config["mds_settings"].keys()
    ranges = []
    for k in keys:
        r_config = config["mds_settings"][k]
        parsed_r = [CommonUtils.parse_si_unit(v) for v in r_config]

        if len(parsed_r) == 3:
            ranges.append(range(*parsed_r))
        elif len(parsed_r) in [1, 2] and all(
            isinstance(x, int) and x < 1000 for x in parsed_r
        ):
            ranges.append(range(*parsed_r))
        else:
            ranges.append(parsed_r)

    for values in itertools.product(*ranges):
        current_settings = dict(zip(keys, values))
        print(f"\n--- Starting Test Iteration: {current_settings} ---")

        # Use the workload runner to determine the results directory
        results_dir = workload_runner.get_results_dir(current_settings, shared_timestamp)

        cephfs_manager.rebuild_filesystem(
            current_settings, ganesha_manager, results_dir
        )
        cephfs_manager.apply_mds_settings(current_settings)

        if config.ganesha_enabled:
            ganesha_manager.provision_ganesha(use_custom=True, results_dir=results_dir)
            mount_manager.nfs_mount()
        else:
            mount_manager.kernel_mount()

        workload_runner.prepare_specstorage()
        workload_runner.run_workload(
            current_settings,
            shared_timestamp,
            cephfs_manager=cephfs_manager,
            ganesha_manager=ganesha_manager,
        )

        mount_manager.unmount_clients()

    # Final cleanup/collection
    if config.get("specstorage", {}).get("lockstat", {}).get("enabled", False):
        for fs in cephfs_manager.fs_names:
            # workload_runner.stop_lockstat(fs)
            pass


if __name__ == "__main__":
    main()
