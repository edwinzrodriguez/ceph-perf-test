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
    """
    Abstract Base Class for Inventory Providers.
    An inventory provider is responsible for supplying host information and global variables
    to the performance test framework.
    """

    @abc.abstractmethod
    def get_hosts(self):
        """
        Returns host information grouped by sections (e.g., 'mons', 'clients', 'ganeshas').

        Returns:
            dict: A dictionary where keys are section names and values are lists of dictionaries.
                  Each host dictionary must contain at least a 'name' key.
                  Example: {'clients': [{'name': 'client-000', 'ansible_ssh_user': 'root'}]}
        """
        pass

    @abc.abstractmethod
    def get_vars(self):
        """
        Returns global variables that can be used for parameter expansion.

        Returns:
            dict: A dictionary of variable names and their values.
        """
        pass

    @abc.abstractmethod
    def get_all_hosts_meta(self):
        """
        Returns a flat mapping of all host names to their metadata.

        Returns:
            dict: A dictionary where keys are host names and values are metadata dictionaries.
        """
        pass


class FSManager(abc.ABC):
    """
    Abstract Base Class for Filesystem Managers.
    A filesystem manager is responsible for managing the filesystem lifecycle,
    including rebuilding the filesystem, applying settings, and managing logging.
    """

    @abc.abstractmethod
    def start_fs_logging(self, loadpoint):
        """Starts MDS debug logging for the specified loadpoint."""
        pass

    @abc.abstractmethod
    def stop_fs_logging(self, loadpoint, results_dir=None):
        """Stops MDS debug logging and collects logs to results_dir."""
        pass

    @abc.abstractmethod
    def rebuild_filesystem(self, settings, ganesha_manager=None, results_dir=None):
        """Rebuilds the filesystem with the specified settings."""
        pass

    @abc.abstractmethod
    def get_fs_names(self):
        """Returns the list of filesystem names."""
        pass

    @abc.abstractmethod
    def apply_fs_settings(self, settings):
        """Applies specific MDS settings to the filesystem."""
        pass

    def safe_json_load(self, raw, default=None):
        """
        Safely loads a JSON string, returning a default value on failure.

        Args:
            raw (str): The raw JSON string to parse.
            default: The default value to return if parsing fails. Defaults to an empty list.

        Returns:
            The parsed JSON data or the default value.
        """
        if default is None:
            default = []
        if not raw or "No services reported" in raw:
            return default
        try:
            return json.loads(raw)
        except:
            return default


class AnsibleInventoryProvider(InventoryProvider):
    """
    Implementation of InventoryProvider that parses an Ansible-style INI inventory file.

    It also loads global variables from 'group_vars/all.yml' and 'cluster.json' if they exist
    relative to the script directory.
    """

    def __init__(self, inventory_path, extra_vars=None):
        self.inventory_path = inventory_path
        self.vars = extra_vars or {}
        self._load_global_vars()
        self.hosts_meta = self._parse_inventory(inventory_path)
        print(f"Loaded inventory from {inventory_path}")
        print(f"Loaded global vars: {self.vars}")
        print(f"Hosts: {self.all_hosts}")
        print(f"Hosts meta: {self.hosts_meta}")

    def _load_global_vars(self):
        base_dir = os.path.dirname(os.path.abspath(__file__))
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
    """
    Implementation of InventoryProvider that uses direct dictionary input.
    Useful for programmatic usage without external inventory files.
    """

    def __init__(self, inventory_dict, vars=None):
        """
        Initializes the provider with a dictionary.
        The dictionary can be in two formats:
        1. Internal format: {'section': [{'name': 'host', 'key': 'val'}]}
        2. YAML format: {'section': {'host': {'key': 'val'}}}
        """
        self.vars = vars or {}
        self.hosts_meta = {}
        self.all_hosts = {}

        for section, hosts in inventory_dict.items():
            self.hosts_meta[section] = []
            if isinstance(hosts, list):
                # Internal format
                for h in hosts:
                    self.hosts_meta[section].append(h)
                    self.all_hosts.setdefault(h["name"], {}).update(h)
            elif isinstance(hosts, dict):
                # YAML format
                for host_name, meta in hosts.items():
                    h = {"name": host_name}
                    h.update(meta)
                    self.hosts_meta[section].append(h)
                    self.all_hosts.setdefault(host_name, {}).update(h)

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
        # The first 'mons' host is used as the admin host to drive tests
        return self.mons[0] if self.mons else None

    @property
    def ganesha_enabled(self):
        return self._config.get("ganesha", {}).get("enabled", False)

    @property
    def ganesha_type(self):
        return self._config.get("ganesha", {}).get("type", "cephadm")

    @property
    def ganesha_binary_path(self):
        return self._config.get("ganesha", {}).get(
            "binary_path", "/usr/local/ceph/bin/ganesha.nfsd"
        )

    @property
    def ganesha_pid_path(self):
        return self._config.get("ganesha", {}).get("pid_path", "/var/run/ganesha.pid")

    @property
    def ganesha_worker_threads(self):
        return self._config.get("ganesha", {}).get("worker_threads")

    @property
    def ganesha_umask(self):
        return self._config.get("ganesha", {}).get("umask")

    @property
    def ganesha_client_oc(self):
        return self._config.get("ganesha", {}).get("client_oc")

    @property
    def ganesha_async(self):
        return self._config.get("ganesha", {}).get("async")

    @property
    def ganesha_zerocopy(self):
        return self._config.get("ganesha", {}).get("zerocopy")

    @property
    def ganesha_client_oc_size(self):
        return self._config.get("ganesha", {}).get("client_oc_size")

    @property
    def ganesha_user_id(self):
        return self._config.get("ganesha", {}).get("user_id", "admin")

    @property
    def ganesha_keyring_path(self):
        return self._config.get("ganesha", {}).get("keyring_path")

    @property
    def ganesha_ceph_binary_path(self):
        return self._config.get("ganesha", {}).get("ceph_binary_path", "/usr/bin/ceph")

    @property
    def ganesha_perf_record(self):
        return self._config.get("ganesha", {}).get("perf_record", False)

    @property
    def fio(self):
        return self._config.get("fio")

    @property
    def cephfs_tool(self):
        return self._config.get("cephfs_tool")

    @property
    def specstorage(self):
        return self._config.get("specstorage")


class SSHExecutor:
    def __init__(self, all_hosts_meta):
        self.all_hosts = all_hosts_meta

    def get_ssh_details(self, host_name):
        meta = self.all_hosts.get(host_name, {})
        return (
            str(meta.get("ansible_ssh_user", "root")),
            str(meta.get("ansible_ssh_host", host_name)),
            str(meta.get("ansible_ssh_port", "22")),
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

        # Support common unit formats like "128MiB", "1GiB", "100MB", "100k", etc.
        # Normalize: remove "B" and "iB" from the end if they exist, but keep "i" for binary.
        # e.g., "MiB" -> "Mi", "MB" -> "M", "Mi" -> "Mi", "M" -> "M"

        normalized = value.strip()
        if normalized.endswith("iB"):
            normalized = normalized[:-1]  # "MiB" -> "Mi"
        elif normalized.endswith("B"):
            normalized = normalized[:-1]  # "MB" -> "M"

        units = {
            "Pi": 1024**5,
            "Ti": 1024**4,
            "Gi": 1024**3,
            "Mi": 1024**2,
            "Ki": 1024,
            "P": 1000**5,
            "T": 1000**4,
            "G": 1000**3,
            "M": 1000**2,
            "K": 1000,
            "p": 1000**5,
            "t": 1000**4,
            "g": 1000**3,
            "m": 1000**2,
            "k": 1000,
        }

        for unit, mult in units.items():
            if normalized.endswith(unit):
                try:
                    num_part = normalized[: -len(unit)].strip()
                    if not num_part:
                        return value
                    return int(num_part) * mult
                except (ValueError, TypeError):
                    continue
        try:
            return int(normalized)
        except (ValueError, TypeError):
            return value

    @staticmethod
    def snake_to_pascal(snake_str):
        return "".join(word.capitalize() for word in snake_str.split("_"))

    @staticmethod
    def get_short_name(var_name):
        """Map a human-readable parameter name to its short abbreviation."""
        name_map = {
            "MDS Cache Memory Limit": "m",
            "Filesystem Name": "fs",
            "Number of Filesystems": "nf",
            "Mounts per Filesystem": "mpf",
            "File Size": "s",
            "Threads": "t",
            "Block Size": "bs",
            "I/O Depth": "iod",
            "Read/Write Pattern": "rw",
            "Read/Write Mix (Read %)": "rwmixread",
            "I/O Engine": "ioe",
            "Direct I/O": "d",
            "Buffered I/O": "buf",
            "Create Serialize": "cs",
            "Duration": "dur",
            "Ramp Time": "rt",
            "GTOD Reduce": "gr",
            "Client Object Cache": "oc",
            "Client Object Cache Size": "ocs",
            "Ganesha Worker Threads": "gwt",
            "Ganesha Umask": "gum",
            "Ganesha Client Object Cache": "goc",
            "Ganesha Async": "gas",
            "Ganesha Zero Copy": "gzc",
            "Ganesha Client Object Cache Size": "gocs",
            "Ganesha User ID": "guid",
            "Ganesha Keyring Path": "gkp",
            "Ganesha Ceph Binary Path": "gcbp",
            "Ganesha Enabled": "ge",
            "Workload Runner": "wr",
        }
        return name_map.get(var_name, var_name.replace(" ", "_").replace("/", "_"))

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

    @staticmethod
    def expand_loadpoints(loadpoints):
        import itertools
        import copy

        expanded_list = []
        for lp in loadpoints:
            # Find keys with list values
            list_keys = [k for k, v in lp.items() if isinstance(v, list)]
            if not list_keys:
                expanded_list.append(lp)
                continue

            # Get all combinations of list values
            keys = list_keys
            value_lists = [lp[k] for k in keys]

            for values in itertools.product(*value_lists):
                new_lp = copy.deepcopy(lp)
                for k, v in zip(keys, values):
                    new_lp[k] = v
                expanded_list.append(new_lp)

        return expanded_list

    @staticmethod
    def get_human_readable_settings(settings, lp_cfg=None, config=None):
        """Construct a dictionary of test parameters with human-readable names."""
        params = {}

        # Mapping of internal keys to human-readable names
        name_map = {
            "mds_cache_memory_limit": "MDS Cache Memory Limit",
            "fs_name": "Filesystem Name",
            "num_filesystems": "Number of Filesystems",
            "mounts_per_fs": "Mounts per Filesystem",
            "size": "File Size",
            "threads": "Threads",
            "block-size": "Block Size",
            "iodepth": "I/O Depth",
            "readwrite": "Read/Write Pattern",
            "rwmixread": "Read/Write Mix (Read %)",
            "ioengine": "I/O Engine",
            "direct": "Direct I/O",
            "buffered": "Buffered I/O",
            "create_serialize": "Create Serialize",
            "duration": "Duration",
            "ramp_time": "Ramp Time",
            "gtod_reduce": "GTOD Reduce",
            "client-oc": "Client Object Cache",
            "client-oc-size": "Client Object Cache Size",
            "ganesha_worker_threads": "Ganesha Worker Threads",
            "ganesha_umask": "Ganesha Umask",
            "ganesha_client_oc": "Ganesha Client Object Cache",
            "ganesha_async": "Ganesha Async",
            "ganesha_zerocopy": "Ganesha Zero Copy",
            "ganesha_client_oc_size": "Ganesha Client Object Cache Size",
            "ganesha_user_id": "Ganesha User ID",
            "ganesha_keyring_path": "Ganesha Keyring Path",
            "ganesha_ceph_binary_path": "Ganesha Ceph Binary Path",
            "ganesha_enabled": "Ganesha Enabled",
        }

        # Helper to format values
        def format_val(v):
            if isinstance(v, (int, float)):
                return CommonUtils.format_si_units(v)
            return v

        # Add global settings
        for k, v in settings.items():
            name = name_map.get(k, k)
            params[name] = format_val(v)

        # Add Ganesha settings if config is provided or already in settings
        ganesha_keys = [
            "ganesha_enabled",
            "ganesha_worker_threads",
            "ganesha_umask",
            "ganesha_client_oc",
            "ganesha_async",
            "ganesha_zerocopy",
            "ganesha_client_oc_size",
            "ganesha_user_id",
            "ganesha_keyring_path",
            "ganesha_ceph_binary_path",
        ]

        # Check settings first for Ganesha keys
        for k in ganesha_keys:
            if k in settings:
                name = name_map.get(k, k)
                params[name] = format_val(settings[k])

        if config and config.ganesha_enabled:
            for k in ganesha_keys:
                if k not in settings:  # Don't overwrite if already added from settings
                    val = getattr(config, k, None)
                    if val is not None:
                        name = name_map.get(k, k)
                        params[name] = format_val(val)

        # Add loadpoint-specific settings (overriding globals if necessary)
        if lp_cfg:
            for k, v in lp_cfg.items():
                name = name_map.get(k, k)
                params[name] = format_val(v)

        return params

    @staticmethod
    def get_workload_base_name(
        workload, output_type, client, lp, settings, lp_cfg=None, config=None
    ):
        exclude = {
            "results_dir",
            "fs_name",
            "executable_path",
            "ceph_args",
            "config_path",
            "keyring",
            "client_id",
            "root_path",
            "ganesha_enabled",
            "ganesha_worker_threads",
            "ganesha_umask",
            "ganesha_client_oc",
            "ganesha_async",
            "ganesha_zerocopy",
            "ganesha_client_oc_size",
            "ganesha_user_id",
            "ganesha_keyring_path",
            "ganesha_ceph_binary_path",
            "workload_dir",
            "run_name",
            "num_filesystems",
            "mounts_per_fs",
            "perf_record",
            "perf_record_script",
            "perf_record_executable",
            "perf_record_duration",
            "lockstat",
        }
        mds_p = "-".join(
            f"{k}{CommonUtils.format_si_units(v)}"
            for k, v in sorted(settings.items())
            if k not in exclude
        )

        g_p = ""
        if config and config.ganesha_enabled:
            g_parts = []
            if config.ganesha_worker_threads:
                g_parts.append(f"{CommonUtils.get_short_name('Ganesha Worker Threads')}{config.ganesha_worker_threads}")
            if config.ganesha_umask:
                g_parts.append(f"{CommonUtils.get_short_name('Ganesha Umask')}{config.ganesha_umask}")
            if config.ganesha_client_oc is not None:
                g_parts.append(f"{CommonUtils.get_short_name('Ganesha Client Object Cache')}{1 if config.ganesha_client_oc else 0}")
            if config.ganesha_async is not None:
                g_parts.append(f"{CommonUtils.get_short_name('Ganesha Async')}{1 if config.ganesha_async else 0}")
            if config.ganesha_zerocopy is not None:
                g_parts.append(f"{CommonUtils.get_short_name('Ganesha Zero Copy')}{1 if config.ganesha_zerocopy else 0}")
            if config.ganesha_client_oc_size:
                g_parts.append(
                    f"{CommonUtils.get_short_name('Ganesha Client Object Cache Size')}{CommonUtils.format_si_units(config.ganesha_client_oc_size)}"
                )
            if g_parts:
                g_p = "-" + "-".join(g_parts)

        lp_str = f"lp{int(lp):02d}" if lp is not None else "lp00"

        parts = []
        if lp_cfg:
            if "size" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('File Size')}{lp_cfg['size']}")
            if "threads" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('Threads')}{lp_cfg['threads']}")
            if "client-oc" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('Client Object Cache')}{lp_cfg['client-oc']}")
            if "client-oc-size" in lp_cfg:
                parts.append(
                    f"{CommonUtils.get_short_name('Client Object Cache Size')}{CommonUtils.format_si_units(lp_cfg['client-oc-size'])}"
                )
            if "block-size" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('Block Size')}{CommonUtils.format_si_units(lp_cfg['block-size'])}")
            if "iodepth" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('I/O Depth')}{lp_cfg['iodepth']}")
            if "readwrite" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('Read/Write Pattern')}{lp_cfg['readwrite']}")
            if "ioengine" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('I/O Engine')}{lp_cfg['ioengine']}")
            if "direct" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('Direct I/O')}{lp_cfg['direct']}")
            if "buffered" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('Buffered I/O')}{lp_cfg['buffered']}")
            if "create_serialize" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('Create Serialize')}{lp_cfg['create_serialize']}")
            # if "gtod_reduce" in lp_cfg:
            #     parts.append(f"gr{lp_cfg['gtod_reduce']}")
            # elif "gtod_reduce" in settings:
            #     parts.append(f"gr{settings['gtod_reduce']}")
            # if "ramp_time" in lp_cfg:
            #     parts.append(f"rt{lp_cfg['ramp_time']}")
            # elif "ramp_time" in settings:
            #     parts.append(f"rt{settings['ramp_time']}")

        options = mds_p + g_p
        if parts:
            options += f"_{'_'.join(parts)}"

        return f"{workload}_{output_type}_{client}_{lp_str}_{options}"
