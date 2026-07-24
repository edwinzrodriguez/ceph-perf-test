import abc
import json
from cephfs_perf_lib import FSManager


class GaneshaManager(abc.ABC):
    def __init__(self, executor, config, fs_manager):
        self.executor = executor
        self.config = config
        self.fs_manager = fs_manager
        self.ganeshas = config.ganeshas
        self.admin = config.admin_host
        self._provisioned = False

    @abc.abstractmethod
    def provision_ganesha(self, use_custom=True, results_dir=None):
        pass

    @abc.abstractmethod
    def cleanup_ganesha(self):
        pass

    @staticmethod
    def get_ganesha_config_str(settings):
        from cephfs_perf_lib import CommonUtils

        parts = []
        if "worker_threads" in settings:
            parts.append(f"{CommonUtils.get_short_name('Ganesha Worker Threads')}{settings['worker_threads']}")
        if "umask" in settings:
            parts.append(f"{CommonUtils.get_short_name('Ganesha Umask')}{settings['umask']}")
        if "client_oc" in settings:
            parts.append(f"{CommonUtils.get_short_name('Ganesha Client Object Cache')}{CommonUtils.format_config_value(settings['client_oc'])}")
        if "syncdataonly" in settings:
            parts.append(f"{CommonUtils.get_short_name('Ganesha Sync Data Only')}{CommonUtils.format_config_value(settings['syncdataonly'])}")
        if "async" in settings:
            parts.append(f"{CommonUtils.get_short_name('Ganesha Async')}{CommonUtils.format_config_value(settings['async'])}")
        if "zerocopy" in settings:
            parts.append(f"{CommonUtils.get_short_name('Ganesha Zero Copy')}{CommonUtils.format_config_value(settings['zerocopy'])}")
        if "client_oc_size" in settings:
            size_str = CommonUtils.format_si_units(
                CommonUtils.parse_si_unit(settings["client_oc_size"])
            )
            parts.append(f"{CommonUtils.get_short_name('Ganesha Client Object Cache Size')}{size_str}")
        if "msgr_workers" in settings:
            parts.append(f"{CommonUtils.get_short_name('Ganesha Msgr Workers')}{settings['msgr_workers']}")
        if "rpc_ioq_thrdmin" in settings:
            parts.append(f"{CommonUtils.get_short_name('Ganesha RPC IOQ Thread Min')}{settings['rpc_ioq_thrdmin']}")
        if "rpc_ioq_thrdmax" in settings:
            parts.append(f"{CommonUtils.get_short_name('Ganesha RPC IOQ Thread Max')}{settings['rpc_ioq_thrdmax']}")
        return "_".join(parts)

    def safe_json_load(self, raw, default=None):
        return FSManager.safe_json_load(self, raw, default)

    def get_fs_names(self):
        """Return list of filesystem names. Delegated to FSManager."""
        return self.fs_manager.get_fs_names()

    def _get_ceph_args(self, include_keyring=True):
        args = []
        if self.config.ceph_conf_path:
            args.append(f"-c {self.config.ceph_conf_path}")
        if self.config.ganesha_user_id:
            args.append(f"--user {self.config.ganesha_user_id}")
        if include_keyring and self.config.ganesha_keyring_path:
            args.append(f"--keyring {self.config.ganesha_keyring_path}")
        return " ".join(args)

    def _get_asok_path(self, host_name):
        cmd = (
            "ls /var/run/ceph/ganesha-*.asok | grep -v 'client.admin.asok' | head -n 1"
        )
        asok_path = self.executor.run_remote(host_name, cmd).strip()
        if not asok_path or "No such file" in asok_path:
            return None
        return asok_path

    def reset_ganesha_perf(self, host_name):
        asok_path = self._get_asok_path(host_name)
        if not asok_path:
            print(f"[{host_name}] Warning: Ganesha admin socket not found for reset.")
            return
        print(f"[{host_name}] Resetting Ganesha perf counters via {asok_path}...")
        self.executor.run_remote(
            host_name,
            f"sudo {self.config.ganesha_ceph_binary_path} {self._get_ceph_args(include_keyring=False)} --admin-daemon {asok_path} perf reset all",
        )

    def collect_ganesha_perf_dump(self, host_name):
        asok_path = self._get_asok_path(host_name)
        if not asok_path:
            print(f"[{host_name}] Warning: Ganesha admin socket not found.")
            return None
        print(f"[{host_name}] Collecting Ganesha perf dump from {asok_path}...")
        dump_raw = self.executor.run_remote(
            host_name,
            f"sudo {self.config.ganesha_ceph_binary_path} {self._get_ceph_args(include_keyring=False)} --admin-daemon {asok_path} perf dump",
        )
        return self.safe_json_load(dump_raw, default=None)

    def start_lockstat(self, host_name):
        asok_path = self._get_asok_path(host_name)
        if not asok_path:
            print(
                f"[{host_name}] Warning: Ganesha admin socket not found for lockstat start."
            )
            return
        print(f"[{host_name}] Starting Ganesha lockstat via {asok_path}...")
        self.executor.run_remote(
            host_name,
            f"{self.config.ganesha_lockstat_path} {asok_path} start",
        )

    def stop_lockstat(self, host_name):
        asok_path = self._get_asok_path(host_name)
        if not asok_path:
            print(
                f"[{host_name}] Warning: Ganesha admin socket not found for lockstat stop."
            )
            return
        print(f"[{host_name}] Stopping Ganesha lockstat via {asok_path}...")
        self.executor.run_remote(
            host_name,
            f"{self.config.ganesha_lockstat_path} {asok_path} stop",
        )

    def reset_lockstat(self, host_name):
        asok_path = self._get_asok_path(host_name)
        if not asok_path:
            print(
                f"[{host_name}] Warning: Ganesha admin socket not found for lockstat reset."
            )
            return
        print(f"[{host_name}] Resetting Ganesha lockstat via {asok_path}...")
        self.executor.run_remote(
            host_name,
            f"{self.config.ganesha_lockstat_path} {asok_path} reset",
        )

    def dump_lockstat(self, host_name):
        asok_path = self._get_asok_path(host_name)
        if not asok_path:
            print(
                f"[{host_name}] Warning: Ganesha admin socket not found for lockstat dump."
            )
            return None

        print(f"[{host_name}] Dumping Ganesha lockstat via {asok_path}...")
        output = self.executor.run_remote(
            host_name,
            f"ceph --admin-daemon {asok_path} lockstat dump 2>&1",
        )
        try:
            return json.loads(output)
        except json.JSONDecodeError as e:
            print(f"[{host_name}] Failed to parse lockstat JSON: {e}")
            print(f"[{host_name}] lockstat dump output: {repr(output)}")
            return {"raw": output, "parse_error": str(e)}
