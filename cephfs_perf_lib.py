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
        return self._config.get("ganesha_yaml_path", "/sfs2020/ganesha.yaml")

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
                h, "test -d /sfs2020 && echo EXISTS || echo MISSING"
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
            spec["extra_container_args"].extend(["-v", "/sfs2020:/sfs2020"])
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


class GaneshaManager:
    def __init__(self, executor, config):
        self.executor = executor
        self.config = config
        self.ganeshas = config.ganeshas
        self.admin = config.admin_host
        self._provisioned = False

    def provision_ganesha(self, use_custom=True, results_dir=None):
        if self._provisioned:
            print("Ganesha already provisioned. Skipping.")
            return
        sid = self.config.ganesha_service_id
        # Ceph CLI might restrict manual creation of pools starting with '.',
        # but the NFS orchestrator often expects it. We'll try to create it,
        # but we'll mainly rely on the orchestrator to handle its own pool if possible.
        self.executor.run_remote(
            self.admin, "sudo ceph osd pool create .nfs --yes-i-really-mean-it || true"
        )
        self.executor.run_remote(
            self.admin, "sudo ceph osd pool application enable .nfs nfs || true"
        )
        if use_custom:
            self.setup_ganesha_config()
        self.generate_ganesha_yaml(sid, self.ganeshas, use_custom)
        ypath = self.config.ganesha_yaml_path
        self.executor.run_remote(self.admin, f"sudo ceph orch apply -i {ypath}")

        # Wait for the NFS service to be running BEFORE applying exports
        print(f"Waiting for NFS service {sid} to be running...")
        for _ in range(30):
            svcs = self.safe_json_load(
                self.executor.run_remote(
                    self.admin, "sudo ceph orch ls --service_type nfs --format json"
                )
            )
            if any(
                s.get("service_id") == sid and s.get("status", {}).get("running", 0) > 0
                for s in svcs
            ):
                break
            time.sleep(10)

        for idx, fs in enumerate(CephFSManager(self.executor, self.config).fs_names):
            exp = {
                "export_id": 100 + idx,
                "path": "/",
                "pseudo": f"/{fs}-export",
                "access_type": "RW",
                "squash": "no_root_squash",
                "protocols": [4],
                "transports": ["TCP"],
                "fsal": {"name": "CEPH", "fs_name": fs, "cmount_path": "/"},
                "clients": [
                    {
                        "addresses": ["*"],
                        "access_type": "RW",
                        "squash": "no_root_squash",
                    }
                ],
            }
            with open(f"/tmp/export_{fs}.json", "w") as f:
                json.dump(exp, f)
            u, h, p = self.executor.get_ssh_details(self.admin)
            subprocess.run(
                [
                    "scp",
                    "-o",
                    "StrictHostKeyChecking=no",
                    "-P",
                    p,
                    f"/tmp/export_{fs}.json",
                    f"{u}@{h}:/sfs2020/export_{fs}.json",
                ]
            )
            # Retry applying export as it may fail if the .nfs pool or NFS cluster is not ready
            for i in range(12):  # Increased retries to 12 (2 mins total)
                try:
                    self.executor.run_remote(
                        self.admin,
                        f"sudo ceph nfs export apply {sid} -i /sfs2020/export_{fs}.json",
                        check=True,
                    )
                    break
                except Exception as e:
                    if i == 11:
                        raise
                    print(f"Retrying export apply for {fs} ({i+1}/12): {e}")
                    time.sleep(10)
        self.executor.run_remote(self.admin, f"sudo ceph orch restart nfs.{sid}")

        # After ganesha starts, run 'config diff' via the admin socket and store results in the output directory
        print("Collecting Ganesha 'config diff' from all ganesha nodes...")
        if results_dir:
            self.executor.run_remote(self.admin, f"mkdir -p {results_dir}")
        for g_host in self.ganeshas:
            cmd = "ls /var/run/ceph/ganesha-*.asok | grep -v 'client.admin.asok' | head -n 1"
            asok_path = self.executor.run_remote(g_host, cmd).strip()

            if not asok_path or "No such file" in asok_path:
                print(f"[{g_host}] Warning: Ganesha admin socket not found for 'config diff'.")
                continue

            print(f"[{g_host}] Running 'config diff' via {asok_path}...")
            diff_output = self.executor.run_remote(g_host, f"sudo ceph --admin-daemon {asok_path} config diff")

            filename = f"ganesha_config_diff_{g_host}.json"
            local_temp = f"/tmp/{filename}"
            with open(local_temp, "w") as f:
                f.write(diff_output)

            u, h, p = self.executor.get_ssh_details(self.admin)
            remote_path = f"{results_dir}/{filename}" if results_dir else f"/sfs2020/{filename}"
            subprocess.run(["scp", "-o", "StrictHostKeyChecking=no", "-P", p, local_temp, f"{u}@{h}:{remote_path}"])
            os.remove(local_temp)
            print(f"[{g_host}] Config diff saved to {self.admin}:{remote_path}")

        self._provisioned = True

    def cleanup_ganesha(self):
        self._provisioned = False
        sid = self.config.ganesha_service_id
        exps = CephFSManager(self.executor, self.config).safe_json_load(
            self.executor.run_remote(
                self.admin, f"sudo ceph nfs export ls {sid} --format json"
            )
        )
        for e in exps:
            self.executor.run_remote(
                self.admin,
                f"sudo ceph nfs export rm {sid} {e.get('path') if isinstance(e, dict) else e}",
            )
        self.executor.run_remote(self.admin, f"sudo ceph orch rm nfs.{sid} || true")

    def safe_json_load(self, raw, default=None):
        return CephFSManager(self.executor, self.config).safe_json_load(raw, default)

    def setup_ganesha_config(self):
        print("Setting up custom Ganesha configuration on ganesha nodes...")
        config_content = (
            "NFS_Core_Param {\n"
            "    Protocols = 4;\n"
            "    Enable_NLM = false;\n"
            "    Enable_RQUOTA = false;\n"
            "    NFS_Port = 2049;\n"
            "    allow_set_io_flusher_fail = true;\n"
            "}\n"
            "NFSv4 {\n"
            '    RecoveryBackend = "rados_cluster";\n'
            "    Minor_Versions = 1, 2;\n"
            "}\n"
            "RADOS_KV {\n"
            "    nodeid = 0;\n"
            '    pool = ".nfs";\n'
            '    namespace = "ganesha";\n'
            '    UserId = "admin";\n'
            "}\n"
            "RADOS_URLS {\n"
            '    UserId = "admin";\n'
            '    watch_url = "rados://.nfs/ganesha/conf-nfs.ganesha";\n'
            "}\n"
            "# Cephadm will still manage exports via the %url include\n"
            "# but we use our custom global settings\n"
            "%%url rados://.nfs/ganesha/conf-nfs.ganesha\n"
        )

        for host_name in self.ganeshas:
            # Create /etc/ceph if it doesn't exist
            self.executor.run_remote(host_name, "sudo mkdir -p /etc/ceph")

            # Using printf to write the config file
            # We need to escape single quotes and other shell-sensitive characters
            escaped_config = config_content.replace("'", "'\\''")
            cmd = f"printf '{escaped_config}' | sudo tee /etc/ceph/ganesha-custom.conf > /dev/null"
            self.executor.run_remote(host_name, cmd)
            self.executor.run_remote(
                host_name, "sudo chmod 0644 /etc/ceph/ganesha-custom.conf"
            )

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

    def generate_ganesha_yaml(self, sid, hosts, custom=False):
        print(f"Generating ganesha.yaml for {sid} (custom_config={custom})...")

        ganesha_spec = {
            "service_type": "nfs",
            "service_id": sid,
            "placement": {"hosts": hosts},
            "spec": {"port": 2049},
        }

        if custom:
            ganesha_spec.update(
                {
                    "extra_container_args": [
                        "-v",
                        "/etc/ceph:/etc/ceph:z",
                        "-v",
                        "/etc/ceph/ganesha-custom.conf:/etc/ganesha/custom.conf:z",
                        "-v",
                        "/var/run/ceph:/var/run/ceph:z",
                        "--env",
                        "GSS_USE_HOSTNAME=0",
                        "--env",
                        "CEPH_CONF=/etc/ceph/ceph.conf",
                        "--env",
                        "CEPH_ARGS=--admin-socket=/var/run/ceph/ganesha-$cluster-$name.asok",
                        "--entrypoint",
                        "/usr/bin/ganesha.nfsd",
                    ],
                    "extra_entrypoint_args": [
                        "-F",
                        "-L",
                        "STDERR",
                        "-N",
                        "NIV_EVENT",
                        "-f",
                        "/etc/ganesha/custom.conf",
                    ],
                }
            )

        local_ganesha_yaml = "ganesha.yaml"
        with open(local_ganesha_yaml, "w") as f:
            yaml.dump(ganesha_spec, f)

        ypath = self.config.ganesha_yaml_path
        u, h, p = self.executor.get_ssh_details(self.admin)
        subprocess.run(
            [
                "scp",
                "-o",
                "StrictHostKeyChecking=no",
                "-P",
                p,
                local_ganesha_yaml,
                f"{u}@{h}:{ypath}",
            ]
        )


class MountManager:
    def __init__(self, executor, config):
        self.executor = executor
        self.config = config
        self.clients = config.clients
        fs_mgr = CephFSManager(executor, config)
        self.fs_names = fs_mgr.fs_names

    def unmount_clients(self):
        for c in self.clients:
            mnts = (
                self.executor.run_remote(
                    c, "awk '$2 ~ \"^/mnt/cephfs_\" {print $2}' /proc/mounts | sort -r"
                )
                .strip()
                .split()
            )
            for m in mnts:
                self.executor.run_remote(
                    c, f"sudo umount -f {m} || sudo umount -l {m} || true"
                )
            self.executor.run_remote(c, "sudo rm -rf /mnt/cephfs_*")

    def kernel_mount(self):
        admin_host = self.config.admin_host
        maddrs = self.executor.run_remote(
            admin_host,
            "sudo ceph mon dump | grep -oE 'v1:[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+:[0-9]+' | head -n 1 | sed 's/v1://'",
        ).strip()
        key = self.executor.run_remote(
            admin_host, "sudo ceph auth get-key client.0"
        ).strip()
        mpfs = self.config.get("specstorage", {}).get("mounts_per_fs", 1)
        for fs in self.fs_names:
            for c in self.clients:
                for i in range(mpfs):
                    p = f"/mnt/cephfs_{fs}" + (f"_{i:02d}" if mpfs > 1 else "")
                    self.executor.run_remote(
                        c,
                        f"sudo mkdir -p {p} && sudo mount -t ceph {maddrs}:/ {p} -o name=0,secret={key},fs={fs}",
                    )
                    u, _, _ = self.executor.get_ssh_details(c)
                    self.executor.run_remote(c, f"sudo chown {u}:{u} {p}")

    def nfs_mount(self):
        gs = self.config.ganeshas
        mpfs = self.config.get("specstorage", {}).get("mounts_per_fs", 1)
        for fs in self.fs_names:
            for idx, c in enumerate(self.clients):
                gh = gs[idx % len(gs)]
                gt = (
                    self.config.all_hosts_meta.get(gh, {}).get("private_ip")
                    or self.executor.get_ssh_details(gh)[1]
                )
                for i in range(mpfs):
                    p = f"/mnt/cephfs_{fs}" + (f"_{i:02d}" if mpfs > 1 else "")
                    self.executor.run_remote(
                        c,
                        f"sudo mkdir -p {p} && sudo mount -t nfs -o nfsvers=4.1,proto=tcp {gt}:/{fs}-export {p}",
                        check=True,
                    )
                    u, _, _ = self.executor.get_ssh_details(c)
                    self.executor.run_remote(c, f"sudo chown {u}:{u} {p}")


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


class SpecStorageWorkloadRunner(WorkloadRunner):
    def run_workload(
        self,
        settings,
        shared_ts=None,
        cephfs_manager=None,
        ganesha_manager=None,
    ):
        cmd = self.config["specstorage"]["run_command"]
        cfg = self.config["specstorage"]["output_path"]
        workload_dir = self.config["specstorage"].get("workload_dir")
        perf_record_enabled = self.config["specstorage"].get("perf_record", False)
        payload = settings.copy()
        payload["fs_name"] = self.config.fs_name
        payload["num_filesystems"] = self.config.num_filesystems
        if workload_dir:
            payload["workload_dir"] = workload_dir
        ts = shared_ts or datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%d-%H%M%S-%f"
        )
        fs_part = f"{self.config.fs_name}-x{len(self.fs_names)}-c{len(self.config.clients)}-m{self.config.get('specstorage', {}).get('mounts_per_fs', 1)}"
        payload["run_name"] = f"{ts}_{fs_part}"
        results_dir = self.get_results_dir(settings, ts)
        if results_dir:
            payload["results_dir"] = results_dir
        settings_json = json.dumps(payload)
        print(f"Running SPECSTORAGE on {self.admin}...")
        user, host, port = self.executor.get_ssh_details(self.admin)
        full_cmd = f"{cmd} -f {cfg} --settings '{settings_json}'"
        print(f"[{self.admin}] Executing: {full_cmd}")
        ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-p", port, f"{user}@{host}", full_cmd]
        current_lp, run_phase_started = 0, False
        perf_triggered, logging_triggered = False, False
        ganesha_perf_enabled = self.config.ganesha_enabled and ganesha_manager
        perf_threads = []
        process = subprocess.Popen(
            ssh_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            stdin=subprocess.DEVNULL,
        )
        output = []
        for line in process.stdout:
            print(f"[{self.admin}] {line}", end="")
            output.append(line)
            if "Starting tests..." in line:
                current_lp += 1
                run_phase_started, perf_triggered, logging_triggered = False, False, False
                print(f"Detected Starting tests... Load Point: {current_lp}")
            if "Starting RUN phase" in line:
                run_phase_started = True
                if self.config.get("specstorage", {}).get("lockstat", {}).get("enabled") and cephfs_manager:
                    print(f"Resetting lockstat for Load Point {current_lp}...")
                    cephfs_manager.reset_lockstat()
                if self.config.get("logging", {}).get("enabled") and not logging_triggered and cephfs_manager:
                    print(f"Triggering MDS logging for Load Point {current_lp}...")
                    cephfs_manager.start_mds_logging(current_lp)
                    logging_triggered = True
                if ganesha_perf_enabled:
                    print(f"Resetting Ganesha perf counters for Load Point {current_lp}...")
                    for g_host in self.config.ganeshas:
                        ganesha_manager.reset_ganesha_perf(g_host)
            if "Tests finished" in line:
                r_dir = payload.get("results_dir")
                if self.config.get("specstorage", {}).get("lockstat", {}).get("enabled") and cephfs_manager:
                    print(f"Dumping lockstat for Load Point {current_lp}...")
                    cephfs_manager.dump_lockstat(current_lp, r_dir)
                if logging_triggered and cephfs_manager:
                    print(f"Stopping MDS logging for Load Point {current_lp}...")
                    cephfs_manager.stop_mds_logging(current_lp, r_dir)
                if ganesha_perf_enabled and r_dir:
                    print(f"Collecting Ganesha perf dumps for Load Point {current_lp}...")
                    lp_tag = f"{int(current_lp):02d}"
                    for g_host in self.config.ganeshas:
                        dump = ganesha_manager.collect_ganesha_perf_dump(g_host)
                        if dump:
                            self.save_json_to_results(f"{g_host}_lp{lp_tag}_ganesha_perf.json", dump, r_dir)
            if perf_record_enabled and run_phase_started and not perf_triggered:
                if "Run " in line and " percent complete" in line:
                    print(f"Triggering perf record for Load Point {current_lp}...")
                    r_dir = payload.get("results_dir")
                    t = threading.Thread(target=self.execute_perf_record, args=(current_lp, r_dir))
                    t.start()
                    perf_threads.append(t)
                    perf_triggered = True
        process.wait()
        for t in perf_threads:
            t.join()
        if process.returncode != 0:
            print(f"Error on {self.admin}: process exited with {process.returncode}")
        return "".join(output)

    def execute_perf_record(self, loadpoint, results_dir=None):
        perf_script = self.config.get("specstorage", {}).get("perf_record_script", "/sfs2020/perf_record.py")
        perf_exe = self.config.get("specstorage", {}).get("perf_record_executable", "ceph-mds")
        perf_dur = self.config.get("specstorage", {}).get("perf_record_duration", 5)
        fg_path = self.config.get("specstorage", {}).get("perf_record_flamegraph_path", "")
        processes = []
        for server_name in self.config.mdss:
            print(f"[{server_name}] Starting parallel perf record for Load Point {loadpoint}")
            fg_arg = f" --flamegraph-path {fg_path}" if fg_path else ""
            u, h, p = self.executor.get_ssh_details(server_name)
            ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-p", p, f"{u}@{h}", f"python3 {perf_script} --loadpoint {loadpoint} --server {server_name} --executable {perf_exe} --duration {perf_dur}{fg_arg}"]
            proc = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=False, stdin=subprocess.DEVNULL)
            processes.append((server_name, proc))

        def collect_output(s_name, p):
            out_bytes, _ = p.communicate()
            out = out_bytes.decode("utf-8", errors="replace")
            if p.returncode != 0:
                print(f"Error on {s_name} during perf record: {out}")
            else:
                if out_bytes:
                    print(f"[{s_name}] Output:\n{out}")
                print(f"[{s_name}] Finished perf record for Load Point {loadpoint}.")

        threads = []
        for s_name, proc in processes:
            t = threading.Thread(target=collect_output, args=(s_name, proc))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        if results_dir:
            print(f"Copying perf reports to {results_dir} on {self.admin}...")
            au, ah, ap = self.executor.get_ssh_details(self.admin)
            for s_name in self.config.mdss:
                check_cmd = f"ls /tmp/perf_report_{s_name}_lp{int(loadpoint):02d}.*"
                try:
                    files = self.executor.run_remote(s_name, check_cmd).strip().split()
                    for f_path in files:
                        self.executor.run_remote(s_name, f"sudo -n chmod 0644 {f_path}")
                        copy_cmd = f"scp -o StrictHostKeyChecking=no -P {ap} {f_path} {au}@{ah}:{results_dir}/"
                        self.executor.run_remote(s_name, copy_cmd)
                        self.executor.run_remote(s_name, f"sudo -n rm -f {f_path}")
                except:
                    print(f"[{s_name}] No report files found for Load Point {loadpoint}, skipping copy.")

    def save_json_to_results(self, filename, data, results_dir):
        local_temp = f"/tmp/{filename}"
        with open(local_temp, "w") as f:
            json.dump(data, f, indent=4)
        u, h, p = self.executor.get_ssh_details(self.admin)
        subprocess.run(["scp", "-o", "StrictHostKeyChecking=no", "-P", p, local_temp, f"{u}@{h}:{results_dir}/"])
        os.remove(local_temp)

    def prepare_storage(self):
        spec_cfg = self.config.get("specstorage", {})
        proto = spec_cfg["prototype"]
        out = spec_cfg["output_path"]
        run_cmd = spec_cfg.get("run_command", "/sfs2020/run_sfs2020_workload.py")
        perf_script = spec_cfg.get("perf_record_script", "/sfs2020/perf_record.py")

        u, h, p = self.executor.get_ssh_details(self.admin)

        # Copy local files to the remote machine
        for local_file, remote_path in [
            ("sfs_rc", proto),
            ("run_workload.py", run_cmd),
            ("perf_record.py", perf_script),
        ]:
            if os.path.exists(local_file):
                print(f"Copying local {local_file} to {remote_path} on {self.admin}...")
                subprocess.run(
                    [
                        "scp",
                        "-o",
                        "StrictHostKeyChecking=no",
                        "-P",
                        p,
                        local_file,
                        f"{u}@{h}:{remote_path}",
                    ]
                )

        mpfs = spec_cfg.get("mounts_per_fs", 1)
        mps = []
        for fs in self.fs_names:
            for i in range(mpfs):
                for c in self.config.clients:
                    mps.append(
                        f"{c}:/mnt/cephfs_{fs}" + (f"_{i:02d}" if mpfs > 1 else "")
                    )
        content = (
            self.executor.run_remote(self.admin, f"cat {proto}")
            + f"\nCLIENT_MOUNTPOINTS={' '.join(mps)}\n"
        )
        with open("/tmp/spec_cfg", "w") as f:
            f.write(content)

        subprocess.run(
            [
                "scp",
                "-o",
                "StrictHostKeyChecking=no",
                "-P",
                p,
                "/tmp/spec_cfg",
                f"{u}@{h}:{out}",
            ]
        )

    def get_results_dir(self, settings, shared_ts=None):
        spec_cfg = self.config.get("specstorage", {})
        base = spec_cfg.get("results_base_dir", "/sfs2020/results")
        ts = shared_ts or datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%d-%H%M%S-%f"
        )
        fs_p = f"{self.config.fs_name}-x{len(self.fs_names)}-c{len(self.config.clients)}-m{spec_cfg.get('mounts_per_fs', 1)}"
        mds_p = "-".join(
            f"{k}{CommonUtils.format_si_units(v)}" for k, v in settings.items()
        )
        return os.path.join(base, f"{ts}_{fs_p}_{mds_p}")


class FioWorkloadRunner(WorkloadRunner):
    def run_workload(
        self,
        settings,
        shared_ts=None,
        cephfs_manager=None,
        ganesha_manager=None,
    ):
        fio_cfg = self.config.fio
        commands = fio_cfg.get("commands", [])
        ts = shared_ts or datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%d-%H%M%S-%f"
        )
        results_dir = self.get_results_dir(settings, ts)

        # Create results directory on admin host
        self.executor.run_remote(self.admin, f"mkdir -p {results_dir}")

        if self.config.ganesha_enabled and ganesha_manager:
            ganesha_manager.provision_ganesha(results_dir=results_dir)

        mpfs = self.config.get("fio", {}).get("mounts_per_fs", 1)
        mount_points = []
        for fs in self.fs_names:
            for i in range(mpfs):
                mount_points.append(f"/mnt/cephfs_{fs}" + (f"_{i:02d}" if mpfs > 1 else ""))

        output = []
        for idx, cmd_template in enumerate(commands):
            loadpoint = idx + 1
            if self.config.ganesha_enabled and ganesha_manager:
                for g_host in ganesha_manager.ganeshas:
                    ganesha_manager.reset_ganesha_perf(g_host)

            for c in self.config.clients:
                # Ensure the results directory is created on each client
                self.executor.run_remote(c, f"mkdir -p {results_dir}")

                for mp in mount_points:
                    # Variables for template substitution
                    variables = {
                        "mount_point": mp,
                        "client": c,
                        "results_dir": results_dir,
                        "fs_name": self.config.fs_name,
                    }
                    cmd = cmd_template
                    for k, v in variables.items():
                        cmd = cmd.replace(f"{{{k}}}", str(v))

                    # Dynamically append output parameters
                    filename = f"fio_{c}_{self.config.fs_name}_lp{loadpoint}.json"
                    remote_path = f"{results_dir}/{filename}"
                    cmd += f" --group_reporting --output-format=json --output={remote_path}"

                    print(f"[{c}] Running Fio command: {cmd}")
                    res = self.executor.run_remote(c, cmd, check=True)
                    output.append(f"[{c}] {cmd}\n{res}")

                    # Copy the results back from the client to the admin host
                    print(f"[{c}] Copying results to {self.admin}:{remote_path}...")
                    local_temp = f"/tmp/{filename}"
                    cu, ch, cp = self.executor.get_ssh_details(c)
                    subprocess.run(
                        [
                            "scp",
                            "-o",
                            "StrictHostKeyChecking=no",
                            "-P",
                            cp,
                            f"{cu}@{ch}:{remote_path}",
                            local_temp,
                        ]
                    )
                    au, ah, ap = self.executor.get_ssh_details(self.admin)
                    subprocess.run(
                        [
                            "scp",
                            "-o",
                            "StrictHostKeyChecking=no",
                            "-P",
                            ap,
                            local_temp,
                            f"{au}@{ah}:{remote_path}",
                        ]
                    )
                    if os.path.exists(local_temp):
                        os.remove(local_temp)

            if self.config.ganesha_enabled and ganesha_manager:
                for g_host in ganesha_manager.ganeshas:
                    perf_dump = ganesha_manager.collect_ganesha_perf_dump(g_host)
                    if perf_dump:
                        filename = f"ganesha_perf_dump_{g_host}_lp{loadpoint}.json"
                        local_temp = f"/tmp/{filename}"
                        with open(local_temp, "w") as f:
                            json.dump(perf_dump, f)

                        u, h, p = self.executor.get_ssh_details(self.admin)
                        remote_path = f"{results_dir}/{filename}"
                        subprocess.run(
                            [
                                "scp",
                                "-o",
                                "StrictHostKeyChecking=no",
                                "-P",
                                p,
                                local_temp,
                                f"{u}@{h}:{remote_path}",
                            ]
                        )
                        os.remove(local_temp)
                        print(f"[{g_host}] Perf dump saved to {self.admin}:{remote_path}")

        return "\n".join(output)

    def get_results_dir(self, settings, shared_ts=None):
        fio_cfg = self.config.fio
        base = fio_cfg.get("results_base_dir", "/tmp/fio_results")
        ts = shared_ts or datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%d-%H%M%S-%f"
        )
        fs_p = f"{self.config.fs_name}-x{len(self.fs_names)}-c{len(self.config.clients)}-m{fio_cfg.get('mounts_per_fs', 1)}"
        mds_p = "-".join(
            f"{k}{CommonUtils.format_si_units(v)}" for k, v in settings.items()
        )
        return os.path.join(base, f"{ts}_{fs_p}_{mds_p}")

    def prepare_storage(self):
        pass
