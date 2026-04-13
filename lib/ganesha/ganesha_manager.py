import abc
from cephfs_perf_lib import CephFSManager


class GaneshaManager(abc.ABC):
    def __init__(self, executor, config):
        self.executor = executor
        self.config = config
        self.ganeshas = config.ganeshas
        self.admin = config.admin_host
        self._provisioned = False

    @abc.abstractmethod
    def provision_ganesha(self, use_custom=True, results_dir=None):
        pass

    @abc.abstractmethod
    def cleanup_ganesha(self):
        pass

    def safe_json_load(self, raw, default=None):
        return CephFSManager(self.executor, self.config).safe_json_load(raw, default)

    def reset_ganesha_perf(self, host_name):
        cmd = "ls /var/run/ceph/ganesha-*.asok | grep -v 'client.admin.asok' | head -n 1"
        asok_path = self.executor.run_remote(host_name, cmd).strip()
        if not asok_path or "No such file" in asok_path:
            print(f"[{host_name}] Warning: Ganesha admin socket not found for reset.")
            return
        print(f"[{host_name}] Resetting Ganesha perf counters via {asok_path}...")
        self.executor.run_remote(
            host_name, f"sudo ceph --admin-daemon {asok_path} perf reset all"
        )

    def collect_ganesha_perf_dump(self, host_name):
        cmd = "ls /var/run/ceph/ganesha-*.asok | grep -v 'client.admin.asok' | head -n 1"
        asok_path = self.executor.run_remote(host_name, cmd).strip()
        if not asok_path or "No such file" in asok_path:
            print(f"[{host_name}] Warning: Ganesha admin socket not found.")
            return None
        print(f"[{host_name}] Collecting Ganesha perf dump from {asok_path}...")
        dump_raw = self.executor.run_remote(
            host_name, f"sudo ceph --admin-daemon {asok_path} perf dump"
        )
        return self.safe_json_load(dump_raw, default=None)
