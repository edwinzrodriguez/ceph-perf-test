import abc
import datetime
import json
import os
import re
import subprocess
import threading
import time
import yaml


class InventoryProvider(abc.ABC):
    @abc.abstractmethod
    def get_hosts(self):
        pass

    @abc.abstractmethod
    def get_vars(self):
        pass

    @abc.abstractmethod
    def get_all_hosts_meta(self):
        pass


class AnsibleInventoryProvider(InventoryProvider):
    def __init__(self, inventory_path, extra_vars=None):
        self.inventory_path = inventory_path
        self.vars = extra_vars or {}
        self._load_global_vars()
        self.hosts_meta = self._parse_inventory(inventory_path)

    def _load_global_vars(self):
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        for p in [
            os.path.join(base_dir, "group_vars", "all.yml"),
            os.path.join(base_dir, "cluster.json"),
        ]:
            if os.path.exists(p):
                with open(p, "r") as f:
                    data = yaml.safe_load(f) if p.endswith(".yml") else json.load(f)
                    self.vars.update(data or {})

    def _expand_vars(self, value):
        if not isinstance(value, str):
            return value
        pattern = re.compile(r"\{\{\s*(\w+)\s*\}\}")
        for _ in range(5):
            replaced = False

            def sub_cb(m):
                nonlocal replaced
                var_name = m.group(1)
                if var_name in self.vars:
                    replaced = True
                    return str(self.vars[var_name])
                return m.group(0)

            new_value = pattern.sub(sub_cb, value)
            if not replaced or new_value == value:
                break
            value = new_value
        return value

    def _parse_inventory(self, path):
        inventory, all_hosts, current_section = {}, {}, None
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith(("#", ";")):
                    continue
                if line.startswith("[") and line.endswith("]"):
                    current_section = line[1:-1]
                    inventory.setdefault(current_section, [])
                    continue
                if current_section:
                    parts = line.split(None, 1)
                    if not parts:
                        continue
                    host_name = parts[0]
                    meta = {"name": host_name}
                    if len(parts) > 1:
                        kv_pattern = re.compile(
                            r'([a-zA-Z0-9_-]+)=((?:"[^"]*"|\'[^\']*\'|\{\{.*?\}\}|[^\s\'\"])+)'
                        )
                        for m in kv_pattern.finditer(parts[1]):
                            k, v = m.group(1), m.group(2).strip("'\"")
                            meta[k] = self._expand_vars(v)
                    inventory[current_section].append(meta)
                    all_hosts.setdefault(host_name, {}).update(meta)
        self.all_hosts = all_hosts
        return inventory

    def get_hosts(self):
        return self.hosts_meta

    def get_vars(self):
        return self.vars

    def get_all_hosts_meta(self):
        return self.all_hosts


class DirectInventoryProvider(InventoryProvider):
    def __init__(self, hosts_meta, vars=None):
        self.hosts_meta = hosts_meta
        self.vars = vars or {}
        self.all_hosts = {}
        for hosts in hosts_meta.values():
            for h in hosts:
                self.all_hosts.setdefault(h["name"], {}).update(h)

    def get_hosts(self):
        return self.hosts_meta

    def get_vars(self):
        return self.vars

    def get_all_hosts_meta(self):
        return self.all_hosts


class PerformanceTestConfig:
    def __init__(self, config_dict, inventory_provider):
        self._config = config_dict
        self._inventory = inventory_provider
        self.hosts_meta = inventory_provider.get_hosts()
        self.all_hosts_meta = inventory_provider.get_all_hosts_meta()
        self.vars = inventory_provider.get_vars()

    def get(self, key, default=None):
        return self._config.get(key, default)

    def __getitem__(self, key):
        return self._config[key]

    @property
    def fs_name(self):
        return self._config["fs_name"]

    @property
    def num_filesystems(self):
        return self._config.get("num_filesystems", 1)

    @property
    def mds_yaml_path(self):
        return self._config.get("mds_yaml_path", "mds.yaml")

    @property
    def ganesha_yaml_path(self):
        return self._config.get("ganesha_yaml_path", "/cephfs_perf/ganesha.yaml")

    @property
    def ganesha_service_id(self):
        return self._config.get("ganesha", {}).get("service_id", "ganesha")

    @property
    def mons(self):
        return [h["name"] for h in self.hosts_meta.get("mons", [])]

    @property
    def mdss(self):
        return [
            h["name"]
            for h in (self.hosts_meta.get("mdss") or self.hosts_meta.get("osds", []))
        ]

    @property
    def clients(self):
        return [h["name"] for h in self.hosts_meta.get("clients", [])]

    @property
    def ganeshas(self):
        return [h["name"] for h in self.hosts_meta.get("ganeshas", [])]

    @property
    def admin_host(self):
        return self.mons[0] if self.mons else None

    @property
    def ganesha_enabled(self):
        return self._config.get("ganesha", {}).get("enabled", False)

    @property
    def fio(self):
        return self._config.get("fio")

    @property
    def specstorage(self):
        return self._config.get("specstorage")


class SSHExecutor:
    def __init__(self, all_hosts_meta):
        self.all_hosts = all_hosts_meta

    def get_ssh_details(self, host_name):
        meta = self.all_hosts.get(host_name, {})
        return (
            meta.get("ansible_ssh_user", "root"),
            meta.get("ansible_ssh_host", host_name),
            meta.get("ansible_ssh_port", "22"),
        )

    def run_remote(self, host_name, cmd, stream=False, check=False):
        user, host, port = self.get_ssh_details(host_name)
        ssh_target = f"{user}@{host}"
        print(f"[{host_name}] Executing: {cmd}")
        ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-p", port, ssh_target, cmd]
        if stream:
            process = subprocess.Popen(
                ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
            )
            output = []
            for line in process.stdout:
                print(f"[{host_name}] {line}", end="")
                output.append(line)
            process.wait()
            if process.returncode != 0 and check:
                raise Exception(f"Error on {host_name}: {process.returncode}")
            return "".join(output)
        else:
            result = subprocess.run(ssh_cmd, capture_output=True, text=True)
            if result.returncode != 0 and check:
                raise Exception(f"Error on {host_name}: {result.stderr}")
            return result.stdout


class CommonUtils:
    @staticmethod
    def parse_si_unit(value):
        if not isinstance(value, str):
            return value
        units = {
            "Ki": 1024,
            "Mi": 1024**2,
            "Gi": 1024**3,
            "Ti": 1024**4,
            "Pi": 1024**5,
            "k": 1000,
            "m": 1000**2,
            "g": 1000**3,
            "t": 1000**4,
            "p": 1000**5,
        }
        for unit, mult in units.items():
            if value.endswith(unit):
                try:
                    return int(value[: -len(unit)]) * mult
                except:
                    continue
        try:
            return int(value)
        except:
            return value

    @staticmethod
    def snake_to_pascal(snake_str):
        return "".join(word.capitalize() for word in snake_str.split("_"))

    @staticmethod
    def format_si_units(value):
        try:
            val = int(value)
        except:
            return str(value)
        if val > 0 and val % 1024 == 0:
            for unit in ["Ki", "Mi", "Gi", "Ti", "Pi"]:
                val //= 1024
                if val % 1024 != 0 or val < 1024:
                    return f"{val}{unit}"
        val = int(value)
        if val > 0 and val % 1000 == 0:
            for unit in ["k", "m", "g", "t", "p"]:
                val //= 1000
                if val % 1000 != 0 or val < 1000:
                    return f"{val}{unit}"
        return str(value)


class CephFSManager:
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

    def start_mds_logging(self, loadpoint):
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

    def stop_mds_logging(self, loadpoint, results_dir=None):
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
        for fs in self.fs_names:
            for server_name in self.mdss:
                if self.lockstat_exists.get(server_name):
                    print(f"[{server_name}] Resetting lockstat for mds.{fs}")
                    self.executor.run_remote(
                        server_name, f"sudo python3 {lockstat_path} mds.{fs} reset"
                    )

    def dump_lockstat(self, loadpoint, results_dir=None):
        lockstat_cfg = self.config.get("specstorage", {}).get("lockstat", {})
        lockstat_path = lockstat_cfg.get("path", "/usr/local/bin/lockstat.py")
        for fs in self.fs_names:
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

    def safe_json_load(self, raw, default=None):
        if default is None:
            default = []
        if not raw or "No services reported" in raw:
            return default
        try:
            return json.loads(raw)
        except:
            return default

    def rebuild_filesystem(self, settings, ganesha_manager=None, results_dir=None):
        self.executor.run_remote(
            self.admin, "sudo ceph config set mon mon_allow_pool_delete true"
        )
        self.executor.run_remote(
            self.admin, "sudo ceph config set global mon_max_pg_per_osd 1000"
        )
        if self.config.ganesha_enabled and ganesha_manager:
            ganesha_manager.cleanup_ganesha()
        for fs in self.fs_names:
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
        start_idx = (self.fs_names.index(fs) if fs in self.fs_names else 0) % num_mdss
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
                    p,
                    "mds.yaml",
                    f"{u}@{h}:{self.config.mds_yaml_path}",
                ]
            )

    def apply_mds_settings(self, settings):
        for k, v in settings.items():
            if k in ["max_mds", "cpus"]:
                continue
            val = CommonUtils.format_si_units(v)
            for fs in self.fs_names:
                self.executor.run_remote(
                    self.admin, f"sudo ceph fs set {fs} mds_{k} {val}"
                )
        if "max_mds" in settings:
            for fs in self.fs_names:
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




class WorkloadRunner(abc.ABC):
    def __init__(self, executor, config, fs_names):
        self.executor, self.config, self.fs_names = executor, config, fs_names
        self.admin = config.admin_host

    @abc.abstractmethod
    def run_workload(
        self,
        settings,
        shared_ts=None,
        cephfs_manager=None,
        ganesha_manager=None,
    ):
        pass

    @abc.abstractmethod
    def get_results_dir(self, settings, shared_ts=None):
        pass

    @abc.abstractmethod
    def prepare_storage(self):
        pass
