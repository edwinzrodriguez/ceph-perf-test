import subprocess
import time
import yaml
from lib.fs.cephfs_manager import CephFSManager


class CephFSCephadmManager(CephFSManager):
    """CephFS manager that deploys MDS daemons via ``ceph orch`` (cephadm)."""

    def _remove_mds_service(self, fs):
        self._run_ceph(self.admin, f"orch rm mds.{fs} || true")
        for _ in range(24):
            if not any(
                s.get("service_type") == "mds" and s.get("service_id") == fs
                for s in self.safe_json_load(
                    self._run_ceph(self.admin, "orch ls --format json")
                )
            ):
                break
            time.sleep(5)
        self._mds_instances.pop(fs, None)

    def _deploy_mds(self, fs, settings):
        max_mds = settings.get("max_mds", 1) if settings else 1
        self.generate_mds_yaml(fs, max_mds, settings)
        self._run_ceph(
            self.admin, f"orch apply -i {self.config.mds_yaml_path}", check=True
        )
        # Record placement for log collection (best-effort from host list)
        selected_hosts = self._select_mds_hosts(fs, max_mds)
        self._mds_instances[fs] = [(h, fs) for h in selected_hosts]

    def generate_mds_yaml(self, fs, count, settings=None):
        selected_hosts = self._select_mds_hosts(fs, count)
        has_sfs = any(
            "EXISTS"
            in self.executor.run_remote(
                h, "test -d /cephfs_perf/sfs2020 && echo EXISTS || echo MISSING"
            )
            for h in selected_hosts
        )
        spec = {
            "service_type": "mds",
            "service_id": fs,
            "placement": {"hosts": selected_hosts},
            "extra_container_args": [
                "--privileged",
                "--cap-add",
                "SYS_MODULE",
                "-e",
                "ENABLE_LOCKSTAT=true",
                "-v",
                "/sys/kernel/debug:/sys/kernel/debug:rw",
                "-v",
                "/usr/src/kernels:/usr/src/kernels:ro",
                "-v",
                "/usr/lib/modules:/usr/lib/modules:ro",
                "-v",
                "/usr/lib/debug:/usr/lib/debug:ro",
            ],
        }
        if settings and "cpus" in settings:
            spec["extra_container_args"].extend(["--cpus", str(settings["cpus"])])
        if has_sfs:
            spec["extra_container_args"].extend(["-v", "/cephfs_perf:/cephfs_perf"])
        with open("mds.yaml", "w") as f:
            yaml.dump(spec, f)
        if self.config.mds_yaml_path != "mds.yaml":
            u, h, p = self.executor.get_ssh_details(self.admin)
            subprocess.run(
                [
                    "scp",
                    "-o",
                    "StrictHostKeyChecking=no",
                    "-P",
                    str(p),
                    "mds.yaml",
                    f"{u}@{h}:{self.config.mds_yaml_path}",
                ]
            )

    def _collect_mds_logs(self, loadpoint, results_dir):
        if not self.is_mds_logging_enabled():
            return
        lp_tag = f"{int(loadpoint):02d}"
        for server_name in self.mdss:
            fsid = self._run_ceph(server_name, "fsid").strip()
            log_dir = f"/var/log/ceph/{fsid}"
            ps_output = self._run_ceph(
                self.admin,
                f"orch ps --hostname {server_name} --daemon_type mds --format json",
            )
            daemons = self.safe_json_load(ps_output)
            for daemon in daemons:
                daemon_name = daemon.get("daemon_name")
                if not daemon_name:
                    continue
                src_log = f"{log_dir}/ceph-{daemon_name}.log"
                dest_log = f"{server_name}_lp{lp_tag}_{daemon_name}.log"
                self._copy_log_to_results(server_name, src_log, dest_log, results_dir)
