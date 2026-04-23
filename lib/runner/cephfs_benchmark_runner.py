import argparse
import datetime
import itertools
import sys
import yaml
from cephfs_perf_lib import (
    AnsibleInventoryProvider,
    DirectInventoryProvider,
    PerformanceTestConfig,
    SSHExecutor,
    CommonUtils,
    StubFSManager,
)
from lib.mount.mount_manager import StubMountManager
from lib.fs.cephfs_manager import CephFSManager
from lib.ganesha.ganesha_cephadm_manager import GaneshaCephadmManager
from lib.ganesha.ganesha_systemd_manager import GaneshaSystemdManager
from lib.mount.mount_kernel_manager import MountKernelManager
from lib.mount.mount_nfs_manager import MountNfsManager


class BenchRunner:
    def __init__(self, description="CephFS Performance Runner"):
        self.description = description
        self.parser = argparse.ArgumentParser(description=description)
        self.add_arguments()

    def add_arguments(self):
        self.parser.add_argument("config", help="Path to the configuration YAML file")
        self.parser.add_argument(
            "inventory", nargs="?", help="Path to the Ansible inventory file (optional)"
        )
        self.parser.add_argument(
            "--ganesha",
            choices=["cephadm", "systemd"],
            help="Enable Ganesha and specify the type",
        )

    def load_config(self, args):
        with open(args.config, "r") as f:
            config_dict = yaml.safe_load(f)

        if "ganesha" not in config_dict:
            config_dict["ganesha"] = {}

        # Default ganesha.enabled can be handled here or in YAML
        if args.ganesha:
            config_dict["ganesha"]["enabled"] = True
            config_dict["ganesha"]["type"] = args.ganesha
        else:
            config_dict["ganesha"]["enabled"] = False

        if args.inventory:
            inventory_provider = AnsibleInventoryProvider(args.inventory)
        elif "inventory" in config_dict:
            inventory_provider = DirectInventoryProvider(config_dict["inventory"])
        else:
            print(
                "Error: No inventory provided (neither via command line nor in config file)"
            )
            sys.exit(1)

        return PerformanceTestConfig(config_dict, inventory_provider)

    def get_workload_runner(self, executor, config, fs_names):
        """Must be implemented by subclasses to return a WorkloadRunner instance."""
        raise NotImplementedError("Subclasses must implement get_workload_runner")

    def run(self):
        args = self.parser.parse_args()
        config = self.load_config(args)

        executor = SSHExecutor(config.all_hosts_meta)
        if config.fs_manager_type == "StubFSManager":
            cephfs_manager = StubFSManager(config)
        else:
            cephfs_manager = CephFSManager(executor, config)
        fs_names = cephfs_manager.get_fs_names()

        if config.ganesha_enabled:
            if config.ganesha_type == "systemd":
                ganesha_manager = GaneshaSystemdManager(
                    executor, config, cephfs_manager
                )
            elif config.ganesha_type == "cephadm":
                ganesha_manager = GaneshaCephadmManager(
                    executor, config, cephfs_manager
                )
            else:
                raise ValueError(f"Invalid Ganesha type: {config.ganesha_type}")
            mount_manager = MountNfsManager(executor, config, cephfs_manager)
        elif config.mount_manager_type == "StubMountManager":
            ganesha_manager = None
            mount_manager = StubMountManager(executor, config, cephfs_manager)
        else:
            ganesha_manager = None
            mount_manager = MountKernelManager(executor, config, cephfs_manager)

        workload_runner = self.get_workload_runner(executor, config, fs_names)

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
        for k in [
            "worker_threads",
            "umask",
            "client_oc",
            "async",
            "zerocopy",
            "client_oc_size",
        ]:
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

            if current_ganesha_settings:
                config["ganesha"].update(current_ganesha_settings)

            print(f"\n--- Starting Test Iteration: {all_settings} ---")

            results_dir = workload_runner.get_results_dir(
                current_settings, shared_timestamp
            )

            cephfs_manager.rebuild_filesystem(
                current_settings, ganesha_manager, results_dir
            )
            cephfs_manager.apply_fs_settings(current_settings)

            if config.ganesha_enabled:
                ganesha_manager.provision_ganesha(
                    use_custom=True, results_dir=results_dir
                )

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

        # Final cleanup/collection (if applicable)
        self.post_run_cleanup(config, fs_names, workload_runner)

    def post_run_cleanup(self, config, fs_names, workload_runner):
        if config.get("specstorage", {}).get("lockstat", {}).get("enabled", False):
            # for fs in fs_names:
            #     workload_runner.stop_lockstat(fs)
            pass
