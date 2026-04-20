#!/usr/bin/env python3
import sys
import yaml
import datetime
import itertools
from cephfs_perf_lib import (
    AnsibleInventoryProvider,
    DirectInventoryProvider,
    PerformanceTestConfig,
    SSHExecutor,
    CommonUtils,
    FSManager,
)
from lib.fs.cephfs_manager import CephFSManager
from lib.workload.workload_runner import WorkloadRunner
from lib.workload.spec_storage_runner import SpecStorageWorkloadRunner
from lib.workload.fio_runner import FioWorkloadRunner
from lib.workload.cephfs_tool_runner import CephFSToolWorkloadRunner
from lib.ganesha.ganesha_cephadm_manager import GaneshaCephadmManager
from lib.ganesha.ganesha_systemd_manager import GaneshaSystemdManager
from lib.mount.mount_kernel_manager import MountKernelManager
from lib.mount.mount_nfs_manager import MountNfsManager


def main():
    if len(sys.argv) < 2:
        print("Usage: cephfs-perf-runner <config.yaml> [ansible_inventory]")
        sys.exit(1)

    config_path = sys.argv[1]
    inventory_path = sys.argv[2] if len(sys.argv) > 2 else None

    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)

    if inventory_path:
        inventory_provider = AnsibleInventoryProvider(inventory_path)
    elif "inventory" in config_dict:
        inventory_provider = DirectInventoryProvider(config_dict["inventory"])
    else:
        print("Error: No inventory provided (neither via command line nor in config file)")
        sys.exit(1)

    config = PerformanceTestConfig(config_dict, inventory_provider)

    executor = SSHExecutor(config.all_hosts_meta)
    cephfs_manager = CephFSManager(executor, config)
    fs_names = cephfs_manager.get_fs_names()

    if config.ganesha_type == "systemd":
        ganesha_manager = GaneshaSystemdManager(executor, config, cephfs_manager)
    else:
        ganesha_manager = GaneshaCephadmManager(executor, config, cephfs_manager)

    if config.ganesha_enabled:
        mount_manager = MountNfsManager(executor, config, cephfs_manager)
    else:
        mount_manager = MountKernelManager(executor, config, cephfs_manager)

    if config.fio:
        workload_runner = FioWorkloadRunner(executor, config, fs_names)
    elif config.cephfs_tool:
        workload_runner = CephFSToolWorkloadRunner(executor, config, fs_names)
    else:
        workload_runner = SpecStorageWorkloadRunner(executor, config, fs_names)

    # Execute test matrix
    mount_manager.unmount_clients()

    now = datetime.datetime.now(datetime.timezone.utc)
    timestamp = now.strftime("%Y%m%d-%H%M%S")
    unix_ts = int(now.timestamp())
    shared_timestamp = f"{timestamp}-{unix_ts}"

    keys = list(config["mds_settings"].keys())
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

    ganesha_settings_raw = config.get("ganesha", {})
    ganesha_keys = []
    ganesha_ranges = []
    # Relevant CEPH FSAL options that can be iterated
    for k in ["worker_threads", "umask", "client_oc", "async", "zerocopy", "client_oc_size"]:
        if k in ganesha_settings_raw:
            val = ganesha_settings_raw[k]
            if isinstance(val, list):
                ganesha_keys.append(k)
                ganesha_ranges.append(val)

    combined_keys = keys + ganesha_keys
    combined_ranges = ranges + ganesha_ranges

    for values in itertools.product(*combined_ranges):
        all_settings = dict(zip(combined_keys, values))
        current_settings = {k: all_settings[k] for k in keys}
        current_ganesha_settings = {k: all_settings[k] for k in ganesha_keys}
        
        # Merge current_ganesha_settings back into config's ganesha block for runners to use
        # This is a bit tricky as PerformanceTestConfig might be used by reference.
        # We'll update the underlying _config dict if needed, or better, pass it.
        if current_ganesha_settings:
            config["ganesha"].update(current_ganesha_settings)

        print(f"\n--- Starting Test Iteration: {all_settings} ---")

        # Use the workload runner to determine the results directory
        results_dir = workload_runner.get_results_dir(current_settings, shared_timestamp)
        
        cephfs_manager.rebuild_filesystem(
            current_settings, ganesha_manager, results_dir
        )
        cephfs_manager.apply_fs_settings(current_settings)

        if config.ganesha_enabled:
            ganesha_manager.provision_ganesha(use_custom=True, results_dir=results_dir)
            mount_manager.mount()
        else:
            mount_manager.mount()

        workload_runner.prepare_storage()

        workload_runner.run_workload(
            current_settings,
            shared_timestamp,
            cephfs_manager=cephfs_manager,
            ganesha_manager=ganesha_manager,
            results_dir=results_dir,
        )

        mount_manager.unmount_clients()

    # Final cleanup/collection
    if config.get("specstorage", {}).get("lockstat", {}).get("enabled", False):
        for fs in fs_names:
            # workload_runner.stop_lockstat(fs)
            pass


if __name__ == "__main__":
    main()
