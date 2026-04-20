import json
import yaml
import time
import subprocess
from cephfs_perf_lib import CommonUtils, FSManager

class CephFSManager(FSManager):
    def __init__(self, executor, config):
        self.executor = executor
        self.config = config
        self.admin = config.admin_host
        self.fs_name = config.fs_name
        self.num_filesystems = config.num_filesystems
        self.fs_names = (
            [self.fs_name]
            + [f"{self.fs_name}_{i:02d}" for i in range(2, self.num_filesystems + 1)]
            if self.num_filesystems > 1
            else [self.fs_name]
        )
        self.mdss = config.mdss
        self.lockstat_exists = {}

    def start_fs_logging(self, loadpoint):
        debug_mds = self.config.get("logging", {}).get("debug_mds", 20)
        debug_ms = self.config.get("logging", {}).get("debug_ms", 1)
        for server_name in self.mdss:
            print(
                f"[{server_name}] Starting MDS debug logging for Load Point {loadpoint}"
            )
            self.executor.run_remote(
                server_name, f"sudo ceph config set mds debug_mds {debug_mds}"
            )
            self.executor.run_remote(
                server_name, f"sudo ceph config set mds debug_ms {debug_ms}"
            )

    def stop_fs_logging(self, loadpoint, results_dir=None):
        for server_name in self.mdss:
            print(
                f"[{server_name}] Stopping MDS debug logging for Load Point {loadpoint}"
            )
            self.executor.run_remote(server_name, "sudo ceph config set mds debug_mds 1")
            self.executor.run_remote(server_name, "sudo ceph config set mds debug_ms 1")
            if results_dir:
                lp_tag = f"{int(loadpoint):02d}"
                fsid = self.executor.run_remote(server_name, "sudo ceph fsid").strip()
                log_dir = f"/var/log/ceph/{fsid}"
                ps_output = self.executor.run_remote(
                    self.admin,
                    f"sudo ceph orch ps --hostname {server_name} --daemon_type mds --format json",
                )
                daemons = self.safe_json_load(ps_output)
                for daemon in daemons:
                    daemon_name = daemon.get("daemon_name")
                    if not daemon_name:
                        continue
                    src_log = f"{log_dir}/ceph-{daemon_name}.log"
                    dest_log = f"{server_name}_lp{lp_tag}_{daemon_name}.log"
                    self.executor.run_remote(
                        server_name, f"sudo cp {src_log} /tmp/{dest_log}"
                    )
                    user, _, _ = self.executor.get_ssh_details(server_name)
                    self.executor.run_remote(
                        server_name, f"sudo chown {user}:{user} /tmp/{dest_log}"
                    )
                    admin_user, admin_host, admin_port = self.executor.get_ssh_details(
                        self.admin
                    )
                    copy_cmd = f"scp -o StrictHostKeyChecking=no -P {admin_port} /tmp/{dest_log} {admin_user}@{admin_host}:{results_dir}/"
                    self.executor.run_remote(server_name, copy_cmd)
                    self.executor.run_remote(server_name, f"rm -f /tmp/{dest_log}")
                    self.executor.run_remote(server_name, f"sudo truncate -s 0 {src_log}")

    def start_lockstat(self, fs):
        lockstat_cfg = self.config.get("specstorage", {}).get("lockstat", {})
        lockstat_path = lockstat_cfg.get("path", "/usr/local/bin/lockstat.py")
        threshold = lockstat_cfg.get("threshold", 0)
        for server_name in self.mdss:
            if server_name not in self.lockstat_exists:
                check = self.executor.run_remote(
                    server_name,
                    f"test -f {lockstat_path} && echo EXISTS || echo MISSING",
                ).strip()
                self.lockstat_exists[server_name] = "EXISTS" in check
            if self.lockstat_exists[server_name]:
                print(
                    f"[{server_name}] Starting lockstat for mds.{fs} with threshold {threshold}"
                )
                self.executor.run_remote(
                    server_name,
                    f"sudo python3 {lockstat_path} mds.{fs} start --threshold {threshold}",
                )

    def stop_lockstat(self, fs):
        lockstat_cfg = self.config.get("specstorage", {}).get("lockstat", {})
        lockstat_path = lockstat_cfg.get("path", "/usr/local/bin/lockstat.py")
        for server_name in self.mdss:
            if self.lockstat_exists.get(server_name):
                print(f"[{server_name}] Stopping lockstat for mds.{fs}")
                self.executor.run_remote(
                    server_name, f"sudo python3 {lockstat_path} mds.{fs} stop"
                )

    def reset_lockstat(self):
        lockstat_cfg = self.config.get("specstorage", {}).get("lockstat", {})
        lockstat_path = lockstat_cfg.get("path", "/usr/local/bin/lockstat.py")
        for fs in self.get_fs_names():
            for server_name in self.mdss:
                if self.lockstat_exists.get(server_name):
                    print(f"[{server_name}] Resetting lockstat for mds.{fs}")
                    self.executor.run_remote(
                        server_name, f"sudo python3 {lockstat_path} mds.{fs} reset"
                    )

    def dump_lockstat(self, loadpoint, results_dir=None):
        lockstat_cfg = self.config.get("specstorage", {}).get("lockstat", {})
        lockstat_path = lockstat_cfg.get("path", "/usr/local/bin/lockstat.py")
        for fs in self.get_fs_names():
            for server_name in self.mdss:
                if self.lockstat_exists.get(server_name):
                    lp_tag = f"{int(loadpoint):02d}"
                    print(
                        f"[{server_name}] Dumping lockstat for mds.{fs} (Load Point {loadpoint})"
                    )
                    if results_dir:
                        dest_file = (
                            f"{server_name}_lp{lp_tag}_mds.{fs}_lockstat_dump.txt"
                        )
                        temp_file = f"/tmp/{dest_file}"
                        self.executor.run_remote(
                            server_name,
                            f"sudo python3 {lockstat_path} mds.{fs} dump --detail | sudo tee {temp_file} > /dev/null",
                        )
                        user, _, _ = self.executor.get_ssh_details(server_name)
                        self.executor.run_remote(
                            server_name, f"sudo chown {user}:{user} {temp_file}"
                        )
                        admin_user, admin_host, admin_port = self.executor.get_ssh_details(
                            self.admin
                        )
                        copy_cmd = f"scp -o StrictHostKeyChecking=no -P {admin_port} {temp_file} {admin_user}@{admin_host}:{results_dir}/"
                        self.executor.run_remote(server_name, copy_cmd)
                        self.executor.run_remote(server_name, f"rm -f {temp_file}")

    def rebuild_filesystem(self, settings, ganesha_manager=None, results_dir=None):
        self.executor.run_remote(
            self.admin, "sudo ceph config set mon mon_allow_pool_delete true"
        )
        self.executor.run_remote(
            self.admin, "sudo ceph config set global mon_max_pg_per_osd 1000"
        )
        if self.config.ganesha_enabled and ganesha_manager:
            ganesha_manager.cleanup_ganesha()
        for fs in self.get_fs_names():
            self.executor.run_remote(self.admin, f"sudo ceph orch rm mds.{fs} || true")
            for _ in range(24):
                if not any(
                    s.get("service_type") == "mds" and s.get("service_id") == fs
                    for s in self.safe_json_load(
                        self.executor.run_remote(
                            self.admin, "sudo ceph orch ls --format json"
                        )
                    )
                ):
                    break
                time.sleep(5)
            self.executor.run_remote(
                self.admin, f"sudo ceph fs fail {fs} --yes-i-really-mean-it || true"
            )
            self.executor.run_remote(
                self.admin, f"sudo ceph fs rm {fs} --yes-i-really-mean-it || true"
            )
            self.executor.run_remote(
                self.admin,
                f"sudo ceph osd pool delete {fs}_metadata {fs}_metadata --yes-i-really-really-mean-it || true",
            )
            self.executor.run_remote(
                self.admin,
                f"sudo ceph osd pool delete {fs}_data {fs}_data --yes-i-really-really-mean-it || true",
            )
            self.executor.run_remote(
                self.admin, f"sudo ceph osd pool create {fs}_metadata"
            )
            self.executor.run_remote(self.admin, f"sudo ceph osd pool create {fs}_data")
            self.executor.run_remote(
                self.admin, f"sudo ceph fs new {fs} {fs}_metadata {fs}_data"
            )
            self.generate_mds_yaml(fs, settings.get("max_mds", 1), settings)
            self.executor.run_remote(
                self.admin, f"sudo ceph orch apply -i {self.config.mds_yaml_path}"
            )
            for _ in range(60):
                status_raw = self.executor.run_remote(
                    self.admin, f"sudo ceph fs status {fs} --format json"
                )
                status = self.safe_json_load(status_raw, {})
                if isinstance(status, list):
                    status = status[0] if status else {}
                mdsmap = status.get("mdsmap", {})
                if isinstance(mdsmap, dict):
                    if (
                        mdsmap.get("up")
                        or mdsmap.get("up:active")
                        or mdsmap.get("active")
                    ):
                        break
                elif isinstance(mdsmap, list):
                    if any(
                        str(e.get("state", "")).lower() in ["active", "up:active"]
                        or "active" in str(e.get("state", "")).lower()
                        for e in mdsmap
                    ):
                        break
                time.sleep(5)
            self.setup_client_auth(fs)
        self.distribute_keys_and_config()

    def generate_mds_yaml(self, fs, count, settings=None):
        num_mdss = len(self.mdss)
        num_hosts = min(count + 2, num_mdss)
        start_idx = (self.get_fs_names().index(fs) if fs in self.get_fs_names() else 0) % num_mdss
        selected_hosts = [
            self.mdss[(start_idx + i) % num_mdss] for i in range(num_hosts)
        ]
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

    def get_fs_names(self):
        return self.fs_names

    def apply_fs_settings(self, settings):
        for k, v in settings.items():
            if k in ["max_mds", "cpus"]:
                continue
            val = CommonUtils.format_si_units(v)
            for fs in self.get_fs_names():
                self.executor.run_remote(
                    self.admin, f"sudo ceph fs set {fs} mds_{k} {val}"
                )
        if "max_mds" in settings:
            for fs in self.get_fs_names():
                self.executor.run_remote(
                    self.admin, f"sudo ceph fs set {fs} max_mds {settings['max_mds']}"
                )

    def setup_client_auth(self, fs):
        self.executor.run_remote(
            self.admin, f"sudo ceph fs authorize {fs} client.0 / rwps"
        )
        self.executor.run_remote(
            self.admin, "sudo ceph auth get client.0 -o /etc/ceph/ceph.client.0.keyring"
        )

    def distribute_keys_and_config(self):
        targets = self.config.clients + self.config.ganeshas
        for t in targets:
            self.executor.run_remote(t, "sudo mkdir -p /etc/ceph")
            u, h, p = self.executor.get_ssh_details(t)
            files = "/etc/ceph/ceph.conf /etc/ceph/ceph.client.0.keyring /etc/ceph/ceph.client.admin.keyring"
            self.executor.run_remote(
                self.admin,
                f"sudo scp -o StrictHostKeyChecking=no -P {p} {files} {u}@{h}:/tmp/",
            )
            self.executor.run_remote(
                t,
                "sudo mv /tmp/ceph.conf /tmp/ceph.client.0.keyring /tmp/ceph.client.admin.keyring /etc/ceph/ && sudo chmod 0600 /etc/ceph/*.keyring",
            )
