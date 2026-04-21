import abc
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
        parts = []
        if "worker_threads" in settings:
            parts.append(f"gwt{settings['worker_threads']}")
        if "umask" in settings:
            parts.append(f"gum{settings['umask']}")
        if "client_oc" in settings:
            val = 1 if settings["client_oc"] else 0
            parts.append(f"goc{val}")
        if "async" in settings:
            val = 1 if settings["async"] else 0
            parts.append(f"gas{val}")
        if "zerocopy" in settings:
            val = 1 if settings["zerocopy"] else 0
            parts.append(f"gzc{val}")
        if "client_oc_size" in settings:
            from cephfs_perf_lib import CommonUtils

            size_str = CommonUtils.format_si_units(
                CommonUtils.parse_si_unit(settings["client_oc_size"])
            )
            parts.append(f"gocs{size_str}")
        return "_".join(parts)

    def safe_json_load(self, raw, default=None):
        return FSManager.safe_json_load(self, raw, default)

    def get_fs_names(self):
        """Return list of filesystem names. Delegated to FSManager."""
        return self.fs_manager.get_fs_names()

    def reset_ganesha_perf(self, host_name):
        cmd = (
            "ls /var/run/ceph/ganesha-*.asok | grep -v 'client.admin.asok' | head -n 1"
        )
        asok_path = self.executor.run_remote(host_name, cmd).strip()
        if not asok_path or "No such file" in asok_path:
            print(f"[{host_name}] Warning: Ganesha admin socket not found for reset.")
            return
        print(f"[{host_name}] Resetting Ganesha perf counters via {asok_path}...")
        self.executor.run_remote(
            host_name, f"sudo ceph --admin-daemon {asok_path} perf reset all"
        )

    def collect_ganesha_perf_dump(self, host_name):
        cmd = (
            "ls /var/run/ceph/ganesha-*.asok | grep -v 'client.admin.asok' | head -n 1"
        )
        asok_path = self.executor.run_remote(host_name, cmd).strip()
        if not asok_path or "No such file" in asok_path:
            print(f"[{host_name}] Warning: Ganesha admin socket not found.")
            return None
        print(f"[{host_name}] Collecting Ganesha perf dump from {asok_path}...")
        dump_raw = self.executor.run_remote(
            host_name, f"sudo ceph --admin-daemon {asok_path} perf dump"
        )
        return self.safe_json_load(dump_raw, default=None)
