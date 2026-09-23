import abc
import base64
import datetime
import json
import os
import re
import shlex
import subprocess
import threading
import time
import yaml


class RegistryCredentials:
    """Resolve container registry logins from ibm-credentials.env-style files."""

    DEFAULT_REGISTRY_LIST = [
        {
            "url": "quay.io",
            "username": "ibm-credentials.env:QUAY_IO_USERNAME",
            "password": "ibm-credentials.env:QUAY_IO_PASSWORD",
        },
        {
            "url": "quay.ceph.io",
            "username": "ibm-credentials.env:QUAY_CEPH_IO_USERNAME",
            "password": "ibm-credentials.env:QUAY_CEPH_IO_PASSWORD",
        },
        {
            "url": "cp.stg.icr.io",
            "username": "ibm-credentials.env:IBM_CR_USERNAME",
            "password": "ibm-credentials.env:IBM_STG_PASSWORD",
        },
        {
            "url": "cp.icr.io",
            "username": "ibm-credentials.env:IBM_CR_USERNAME",
            "password": "ibm-credentials.env:IBM_CR_PASSWORD",
        },
    ]

    def __init__(self, credentials_file="ibm-credentials.env", registry_list=None):
        self.credentials_file = credentials_file
        self.registry_list = registry_list or self.DEFAULT_REGISTRY_LIST
        self._credentials_cache = None

    @staticmethod
    def load_env_file(path):
        creds = {}
        if not path or not os.path.exists(path):
            return creds
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                value = value.strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
                    value = value[1:-1]
                creds[key.strip()] = value
        return creds

    def _credentials(self):
        if self._credentials_cache is None:
            self._credentials_cache = self.load_env_file(self.credentials_file)
        return self._credentials_cache

    def resolve_value(self, value):
        if not isinstance(value, str):
            return value
        if ":" in value:
            file_part, key_part = value.split(":", 1)
            if file_part.endswith(".env"):
                cred_path = file_part
                if not os.path.isabs(cred_path):
                    cred_path = os.path.join(
                        os.path.dirname(os.path.abspath(self.credentials_file)),
                        os.path.basename(cred_path),
                    )
                creds = self.load_env_file(cred_path)
                return creds.get(key_part, "")
        return value

    def resolved_registry_list(self):
        entries = []
        for item in self.registry_list:
            if not isinstance(item, dict):
                continue
            url = (item.get("url") or "").strip()
            username = self.resolve_value(item.get("username", ""))
            password = self.resolve_value(item.get("password", ""))
            if url and username and password:
                entries.append(
                    {"url": url, "username": username, "password": password}
                )
        return entries

    @staticmethod
    def registry_from_image(image):
        image = (image or "").split("://", 1)[-1]
        return image.split("/", 1)[0]

    def credentials_for_image(self, image):
        host = self.registry_from_image(image)
        for entry in self.resolved_registry_list():
            if host == entry["url"] or image.startswith(entry["url"] + "/"):
                return entry
        return None

    @staticmethod
    def podman_login_command(registry_url, username, password):
        user_b64 = base64.b64encode(username.encode()).decode()
        pass_b64 = base64.b64encode(password.encode()).decode()
        registry = shlex.quote(registry_url)
        return (
            f"user=$(echo {user_b64} | base64 -d); "
            f"pass=$(echo {pass_b64} | base64 -d); "
            f'echo "$pass" | sudo podman login {registry} -u "$user" --password-stdin'
        )


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
    def rebuild_filesystem(
        self, settings, ganesha_manager=None, samba_manager=None, results_dir=None
    ):
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

    @abc.abstractmethod
    def start_lockstat(self, fs):
        """Starts lockstat for the specified filesystem."""
        pass

    @abc.abstractmethod
    def stop_lockstat(self, fs):
        """Stops lockstat for the specified filesystem."""
        pass

    @abc.abstractmethod
    def reset_lockstat(self):
        """Resets lockstat on all MDS nodes."""
        pass

    @abc.abstractmethod
    def dump_lockstat(self, loadpoint, results_dir=None, phase=None, settings=None, lp_cfg=None):
        """Dumps lockstat data to results_dir.

        Extra ``phase``/``settings``/``lp_cfg`` args let workload runners
        annotate dump filenames with the current phase and loadpoint options.
        """
        pass

    def is_mds_lockstat_enabled(self):
        """Return True when MDS-side lockstat collection should be performed.

        Default False so StubFSManager / CephPoolManager can be called
        unconditionally by any workload. CephFSManager overrides to read
        ``mds.lockstat.enabled`` from the config.
        """
        return False

    def is_mds_perf_record_enabled(self):
        """Return True when MDS-side perf-record collection should be performed.

        Default False so pool/stub managers no-op. CephFSManager overrides
        to read ``mds.perf_record`` from the config.
        """
        return False

    def is_mds_logging_enabled(self):
        """Return True when MDS debug logging should be toggled per loadpoint.

        Default False so pool/stub managers no-op. CephFSManager overrides
        to read ``mds.logging.enabled`` from the config.
        """
        return False

    def is_mds_periodic_perf_dump_enabled(self):
        """Return True when periodic MDS perf/histogram dumps should run.

        Default False so pool/stub managers no-op. CephFSManager overrides
        to read ``mds.perf_dump.enabled`` from the config.
        """
        return False

    def start_periodic_perf_dump(self, loadpoint, results_dir=None):
        """Start local MDS perf dump collectors for the loadpoint.

        Default no-op for stub/pool managers. CephFSManager implements this.
        """
        pass

    def stop_periodic_perf_dump(self):
        """Stop local MDS perf dump collectors and collect files.

        Default no-op for stub/pool managers. CephFSManager implements this.
        """
        pass

    def reset_perf_counters(self):
        """Reset MDS admin-socket perf counters (start of loadpoint window).

        Default no-op for stub/pool managers. CephFSManager implements this.
        """
        pass

    def dump_perf_counters(
        self, loadpoint, results_dir=None, phase=None, settings=None, lp_cfg=None
    ):
        """Dump MDS admin-socket perf counters and histograms (end of loadpoint).

        Default no-op for stub/pool managers. CephFSManager implements this.
        """
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


class StubFSManager(FSManager):
    """
    A stub implementation of FSManager that does nothing.
    Useful when runners don't need to rebuild or manage a filesystem.
    """

    def __init__(self, config=None):
        self.config = config
        if config:
            self.fs_name = config.fs_name
            self.num_filesystems = config.num_filesystems
            self.fs_names = (
                [self.fs_name]
                + [f"{self.fs_name}_{i:02d}" for i in range(2, self.num_filesystems + 1)]
                if self.num_filesystems > 1
                else [self.fs_name]
            )
        else:
            self.fs_names = []

    def start_fs_logging(self, loadpoint):
        pass

    def stop_fs_logging(self, loadpoint, results_dir=None):
        pass

    def rebuild_filesystem(
        self, settings, ganesha_manager=None, samba_manager=None, results_dir=None
    ):
        pass

    def get_fs_names(self):
        return self.fs_names

    def apply_fs_settings(self, settings):
        pass

    def start_lockstat(self, fs):
        pass

    def stop_lockstat(self, fs):
        pass

    def reset_lockstat(self):
        pass

    def dump_lockstat(self, loadpoint, results_dir=None, phase=None, settings=None, lp_cfg=None):
        pass

    def reset_perf_counters(self):
        pass

    def dump_perf_counters(
        self, loadpoint, results_dir=None, phase=None, settings=None, lp_cfg=None
    ):
        pass


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
        self._mount_display_name = None

    def set_mount_display_name(self, name):
        self._mount_display_name = name or None

    @property
    def mount_display_name(self):
        return self._mount_display_name

    def get(self, key, default=None):
        return self._config.get(key, default)

    def __getitem__(self, key):
        return self._config[key]

    @property
    def fs_manager_type(self):
        if "fs_manager_type" in self._config:
            return self._config["fs_manager_type"]
        if "rados_bench" in self._config:
            return "CephPoolManager"
        return "CephFSManager"

    @property
    def mount_manager_type(self):
        if "mount_manager_type" in self._config:
            return self._config["mount_manager_type"]
        if "cephfs_tool" in self._config:
            return "StubMountManager"
        if "rados_bench" in self._config:
            return "StubMountManager"
        return "MountKernelManager"

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
    def sambas(self):
        return [h["name"] for h in self.hosts_meta.get("sambas", [])]

    @property
    def rgws(self):
        return [h["name"] for h in self.hosts_meta.get("rgws", [])]

    @property
    def grafanas(self):
        return [h["name"] for h in self.hosts_meta.get("grafanas", [])]

    @property
    def mgrs(self):
        hosts_meta = self.hosts_meta.get("mgrs")
        if hosts_meta:
            return [h["name"] for h in hosts_meta]
        return self.mons

    @property
    def grafana_hosts(self):
        """Grafana/Prometheus placement hosts (grafanas group, else first mon)."""
        if self.grafanas:
            return self.grafanas
        if self.admin_host:
            return [self.admin_host]
        return []

    @property
    def admin_host(self):
        # The first 'mons' host is used as the admin host to drive tests
        return self.mons[0] if self.mons else None

    @property
    def ceph_conf_path(self):
        return self._config.get("ceph", {}).get("conf", "/etc/ceph/ceph.conf")

    @property
    def ceph_keyring_path(self):
        return self._config.get("ceph", {}).get("keyring")

    @property
    def ceph_user_id(self):
        return self._config.get("ceph", {}).get("user_id", "admin")

    @property
    def ceph_fsid(self):
        return self._config.get("ceph", {}).get("fsid")

    @property
    def ganesha_enabled(self):
        return self._config.get("ganesha", {}).get("enabled", False)

    @property
    def ganesha_type(self):
        return self._config.get("ganesha", {}).get("type", "cephadm")

    @property
    def ganesha_binary_path(self):
        return self.expand_env(
            self._config.get("ganesha", {}).get(
                "binary_path", "/usr/local/ceph/bin/ganesha.nfsd"
            )
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
    def ganesha_syncdataonly(self):
        return self._config.get("ganesha", {}).get("syncdataonly")

    @property
    def ganesha_client_fsync_to_rados(self):
        return self._config.get("ganesha", {}).get("client_fsync_to_rados")

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
    def ganesha_msgr_workers(self):
        return self._config.get("ganesha", {}).get("msgr_workers")

    @property
    def ganesha_slot_table_size(self):
        return self._config.get("ganesha", {}).get("slot_table_size")

    @property
    def ganesha_rpc_ioq_thrdmin(self):
        return self._config.get("ganesha", {}).get("rpc_ioq_thrdmin")

    @property
    def ganesha_rpc_ioq_thrdmax(self):
        return self._config.get("ganesha", {}).get("rpc_ioq_thrdmax")

    @property
    def ganesha_user_id(self):
        user_id = self._config.get("ganesha", {}).get("user_id")
        if user_id:
            return user_id
        return self.ceph_user_id

    @property
    def ganesha_keyring_path(self):
        keyring = self._config.get("ganesha", {}).get("keyring_path")
        if keyring:
            return keyring
        return self.ceph_keyring_path

    @property
    def env_vars(self):
        """Top-level env_vars used as the base for all tool invocations."""
        return dict(self._config.get("env_vars") or {})

    def get_merged_env_vars(self, *extra_dicts):
        """Merge global env_vars with additional dicts (later entries win)."""
        merged = self.env_vars
        for d in extra_dicts:
            if d:
                merged.update(d)
        return merged

    def expand_env(self, value, *extra_env_dicts):
        """Expand $VAR / ${VAR} in value using top-level (and optional extra) env_vars.

        Used for config paths like ``${CEPH_INSTALL_PREFIX}/bin/ceph`` that are
        not themselves env_vars entries.
        """
        known = CommonUtils.expand_env_vars_map(
            self.get_merged_env_vars(*extra_env_dicts)
        )
        return CommonUtils.expand_env_value(value, known)

    def resolve_ceph_binary(self, section=None):
        """Resolve the ``ceph`` CLI for cluster commands.

        Honors ``ceph_binary_path`` from *section*, then ``mds``, then
        ``ganesha``, expanding ``${CEPH_INSTALL_PREFIX}`` etc. from env_vars.
        When unset, returns bare ``ceph`` so PATH from env_vars selects the
        build under test.
        """
        candidates = []
        if section:
            candidates.append((self.get(section) or {}).get("ceph_binary_path"))
        mds_cfg = self.get("mds", {}) or {}
        candidates.append(mds_cfg.get("ceph_binary_path"))
        ganesha_cfg = self.get("ganesha", {}) or {}
        candidates.append(ganesha_cfg.get("ceph_binary_path"))
        for path in candidates:
            if path:
                return self.expand_env(path)
        return "ceph"

    @property
    def ganesha_env_vars(self):
        return self._config.get("ganesha", {}).get("env_vars", {})

    @property
    def ganesha_client_log_level(self):
        return self._config.get("ganesha", {}).get("client_log_level", 1)

    @property
    def ganesha_finisher_log_level(self):
        return self._config.get("ganesha", {}).get("finisher_log_level")

    @property
    def ganesha_log_level(self):
        return self._config.get("ganesha", {}).get("log_level")

    @property
    def ganesha_ceph_binary_path(self):
        return self.expand_env(
            self._config.get("ganesha", {}).get("ceph_binary_path", "/usr/bin/ceph")
        )

    @property
    def ganesha_lockstat_path(self):
        return self.expand_env(
            self._config.get("ganesha", {}).get("lockstat", {}).get("path")
            or self._config.get("specstorage", {}).get("lockstat", {}).get("path")
            or "ceph-lockstat"
        )

    @property
    def ganesha_perf_record(self):
        return self._config.get("ganesha", {}).get("perf_record", False)

    @property
    def samba_enabled(self):
        return self._config.get("samba", {}).get("enabled", False)

    @property
    def samba_type(self):
        return self._config.get("samba", {}).get("type", "cephadm")

    @property
    def samba_cluster_id(self):
        return self._config.get("samba", {}).get("cluster_id", "samba")

    @property
    def samba_clustering(self):
        return self._config.get("samba", {}).get("clustering", "never")

    @property
    def samba_share_prefix(self):
        return self._config.get("samba", {}).get("share_prefix", "")

    @property
    def samba_username(self):
        return self._config.get("samba", {}).get("username", "cephuser")

    @property
    def samba_password(self):
        return self._config.get("samba", {}).get("password", "cephpass")

    @property
    def samba_workgroup(self):
        return self._config.get("samba", {}).get("workgroup", "WORKGROUP")

    @property
    def samba_mount_base(self):
        return self._config.get("samba", {}).get("mount_base", "/srv/samba")

    @property
    def samba_config_path(self):
        return self._config.get("samba", {}).get("config_path", "/etc/samba/smb.conf")

    @property
    def samba_user_id(self):
        user_id = self._config.get("samba", {}).get("user_id")
        if user_id:
            return user_id
        return self.ceph_user_id

    @property
    def samba_keyring_path(self):
        keyring = self._config.get("samba", {}).get("keyring_path")
        if keyring:
            return keyring
        return self.ceph_keyring_path

    @property
    def samba_env_vars(self):
        return self._config.get("samba", {}).get("env_vars", {})

    @property
    def samba_ceph_binary_path(self):
        return self.expand_env(
            self._config.get("samba", {}).get("ceph_binary_path", "/usr/bin/ceph")
        )

    @property
    def samba_resources_path(self):
        return self._config.get(
            "samba", {}
        ).get("samba_resources_path", "/cephfs_perf/samba/resources.yaml")

    @property
    def samba_subvolume_group(self):
        return self._config.get("samba", {}).get("subvolume_group", "smb")

    @property
    def samba_subvolume_mode(self):
        return self._config.get("samba", {}).get("subvolume_mode", "0777")

    @property
    def samba_ceph_vfs(self):
        return self._config.get("samba", {}).get("ceph_vfs", False)

    @property
    def samba_ceph_conf_dir(self):
        return self._config.get("samba", {}).get("ceph_conf_dir", "/etc/ceph")

    @property
    def samba_client_log_level(self):
        return self._config.get("samba", {}).get("client_log_level", 1)

    @property
    def samba_finisher_log_level(self):
        return self._config.get("samba", {}).get("finisher_log_level")

    @property
    def samba_client_oc_size(self):
        return self._config.get("samba", {}).get("client_oc_size")

    @property
    def samba_msgr_workers(self):
        return self._config.get("samba", {}).get("msgr_workers")

    @property
    def rgw_enabled(self):
        return self._config.get("rgw", {}).get("enabled", False)

    @property
    def rgw_type(self):
        rgw_cfg = self._config.get("rgw", {}) or {}
        explicit = rgw_cfg.get("type")
        if explicit:
            return explicit
        if self.fs_manager_type == "CephFSSystemdManager":
            return "systemd"
        return "cephadm"

    @property
    def rgw_service_id(self):
        return self._config.get("rgw", {}).get("service_id", "rgw")

    @property
    def rgw_host_label(self):
        return self._config.get("rgw", {}).get("host_label", "rgw")

    @property
    def rgw_count_per_host(self):
        return self._config.get("rgw", {}).get("count_per_host", 1)

    @property
    def rgw_frontend_port(self):
        return self._config.get("rgw", {}).get("frontend_port", 7480)

    @property
    def rgw_yaml_path(self):
        return self._config.get("rgw", {}).get(
            "yaml_path", "/cephfs_perf/rgw.yaml"
        )

    @property
    def rgw_ceph_binary_path(self):
        return self.expand_env(
            self._config.get("rgw", {}).get(
                "ceph_binary_path", "${CEPH_INSTALL_PREFIX}/bin/ceph"
            )
        )

    @property
    def rgw_radosgw_binary_path(self):
        return self.expand_env(
            self._config.get("rgw", {}).get(
                "radosgw_binary_path",
                "${CEPH_INSTALL_PREFIX}/bin/radosgw",
            )
        )

    @property
    def rgw_radosgw_admin_binary_path(self):
        return self.expand_env(
            self._config.get("rgw", {}).get(
                "radosgw_admin_binary_path",
                "${CEPH_INSTALL_PREFIX}/bin/radosgw-admin",
            )
        )

    @property
    def rgw_pid_dir(self):
        return self._config.get("rgw", {}).get("pid_dir", "/var/run/ceph")

    @property
    def rgw_user_id(self):
        user_id = self._config.get("rgw", {}).get("user_id")
        if user_id:
            return user_id
        return self.ceph_user_id

    @property
    def rgw_keyring_path(self):
        keyring = self._config.get("rgw", {}).get("keyring_path")
        if keyring:
            return keyring
        return self.ceph_keyring_path

    @property
    def rgw_env_vars(self):
        return self._config.get("rgw", {}).get("env_vars", {})

    @property
    def rgw_uid(self):
        return self._config.get("rgw", {}).get("uid", "perfuser")

    @property
    def rgw_display_name(self):
        return self._config.get("rgw", {}).get("display_name", "Perf User")

    @property
    def rgw_access_key(self):
        return self._config.get("rgw", {}).get("access_key")

    @property
    def rgw_secret_key(self):
        return self._config.get("rgw", {}).get("secret_key")

    @property
    def rgw_credentials_path(self):
        return self._config.get("rgw", {}).get(
            "credentials_path", "/cephfs_perf/rgw/s3_credentials.json"
        )

    @property
    def rgw_manage_firewall(self):
        return self._config.get("rgw", {}).get("manage_firewall", True)

    @property
    def fio(self):
        return self._config.get("fio")

    @property
    def cephfs_tool(self):
        return self._config.get("cephfs_tool")

    @property
    def rados_bench(self):
        return self._config.get("rados_bench")

    @property
    def rbd(self):
        return self._config.get("rbd")

    @property
    def specstorage(self):
        return self._config.get("specstorage")

    @property
    def grafana_enabled(self):
        return self._config.get("grafana", {}).get("enabled", False)

    @property
    def grafana_type(self):
        grafana_cfg = self._config.get("grafana", {}) or {}
        explicit = grafana_cfg.get("type")
        if explicit:
            return explicit
        if self.fs_manager_type == "CephFSSystemdManager":
            return "systemd"
        return "cephadm"

    @property
    def grafana_exporter_prio_limit(self):
        return self._config.get("grafana", {}).get("exporter_prio_limit", 5)

    @property
    def grafana_yaml_path(self):
        return self._config.get("grafana", {}).get(
            "yaml_path", "/cephfs_perf/monitoring.yaml"
        )

    @property
    def grafana_ceph_binary_path(self):
        return self.expand_env(
            self._config.get("grafana", {}).get(
                "ceph_binary_path", "${CEPH_INSTALL_PREFIX}/bin/ceph"
            )
        )

    @property
    def grafana_env_vars(self):
        return self._config.get("grafana", {}).get("env_vars", {})

    @property
    def grafana_port(self):
        return self._config.get("grafana", {}).get("port", 3000)

    @property
    def prometheus_port(self):
        return self._config.get("grafana", {}).get("prometheus_port", 9095)

    @property
    def prometheus_scrape_interval(self):
        return self._config.get("grafana", {}).get("prometheus_scrape_interval", "15s")

    @property
    def prometheus_cluster_label(self):
        """Value for the Prometheus ``cluster`` label (Ceph dashboards filter on it)."""
        return self._config.get("grafana", {}).get("prometheus_cluster_label", "ceph")

    @property
    def grafana_exporter_port(self):
        return self._config.get("grafana", {}).get("exporter_port", 9926)

    @property
    def grafana_mgr_prometheus_port(self):
        return self._config.get("grafana", {}).get("mgr_prometheus_port", 9283)

    @property
    def grafana_anonymous_access(self):
        return self._config.get("grafana", {}).get("anonymous_access", True)

    @property
    def grafana_timezone(self):
        """Grafana UI timezone. UTC matches Ceph daemon log timestamps."""
        return self._config.get("grafana", {}).get("timezone", "UTC")

    @property
    def grafana_protocol(self):
        return self._config.get("grafana", {}).get("protocol", "http")

    @property
    def grafana_ssl(self):
        return self._config.get("grafana", {}).get("ssl", False)

    @property
    def grafana_credentials_file(self):
        path = self._config.get("grafana", {}).get(
            "credentials_file", "ibm-credentials.env"
        )
        if os.path.isabs(path):
            return path
        return os.path.abspath(path)

    @property
    def grafana_registry_list(self):
        grafana_cfg = self._config.get("grafana", {}) or {}
        registry_list = grafana_cfg.get("registry_list")
        if registry_list is not None:
            return registry_list
        return RegistryCredentials.DEFAULT_REGISTRY_LIST

    @property
    def grafana_registry_credentials(self):
        return RegistryCredentials(
            credentials_file=self.grafana_credentials_file,
            registry_list=self.grafana_registry_list,
        )

    @property
    def grafana_image(self):
        return self._config.get("grafana", {}).get(
            "image", "quay.io/ceph/grafana:12.3.1"
        )

    @property
    def grafana_prometheus_image(self):
        return self._config.get("grafana", {}).get(
            "prometheus_image", "quay.io/prometheus/prometheus:v3.6.0"
        )


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
        # Pass command via stdin to avoid "Argument list too long" errors
        # when cmd contains very long arguments (e.g., long CEPH_ARGS)
        ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-p", port, ssh_target, "bash -s"]
        if stream:
            process = subprocess.Popen(
                ssh_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
            )
            # Send command to stdin and close it
            process.stdin.write(cmd + "\n")
            process.stdin.close()
            output = []
            for line in process.stdout:
                print(f"[{host_name}] {line}", end="")
                output.append(line)
            process.wait()
            if process.returncode != 0 and check:
                raise Exception(f"Error on {host_name}: {process.returncode}")
            return "".join(output)
        else:
            result = subprocess.run(ssh_cmd, input=cmd + "\n", capture_output=True, text=True)
            if result.returncode != 0 and check:
                raise Exception(f"Error on {host_name}: {result.stderr}")
            return result.stdout


class CommonUtils:
    @staticmethod
    def dump_lockstat_common(executor, host_name, loadpoint, results_dir, target_name, dump_cmd, admin_host, settings=None, lp_cfg=None, phase=None):
        """
        Common logic to dump and collect lockstat files.
        """
        output_type = f"lockstat_{phase}" if phase else "lockstat"
        if settings is not None:
            dest_file = f"{CommonUtils.get_workload_base_name(target_name, output_type, host_name, loadpoint, settings, lp_cfg)}.txt"
        else:
            lp_tag = f"{int(loadpoint):02d}"
            phase_suffix = f"_{phase}" if phase else ""
            dest_file = f"{target_name}_lockstat{phase_suffix}_{host_name}_lp{lp_tag}.txt"
        temp_file = f"/tmp/{dest_file}"

        # Execute the dump command and save to temp file
        executor.run_remote(
            host_name,
            f"{dump_cmd} | sudo tee {temp_file} > /dev/null",
        )

        # Change ownership to the current user to allow scp
        user, _, _ = executor.get_ssh_details(host_name)
        executor.run_remote(
            host_name, f"sudo chown {user}:{user} {temp_file}"
        )

        # Get admin host details for scp
        admin_user, admin_host_addr, admin_port = executor.get_ssh_details(admin_host)

        # Copy to results directory on admin host
        copy_cmd = f"scp -o StrictHostKeyChecking=no -P {admin_port} {temp_file} {admin_user}@{admin_host_addr}:{results_dir}/"
        executor.run_remote(host_name, copy_cmd)

        # Cleanup temp file
        executor.run_remote(host_name, f"rm -f {temp_file}")

    @staticmethod
    def collect_journal_logs(executor, hosts, results_dir):
        """Collect the last 5 minutes of journal logs from all specified hosts in parallel."""
        if not results_dir or not os.path.exists(results_dir):
            print(f"Warning: Results directory {results_dir} does not exist. Skipping log collection.")
            return

        print(f"Collecting journal logs from {len(hosts)} hosts for the last 5 minutes into {results_dir}...")
        threads = []

        def collect_host_logs(host):
            try:
                log_file = os.path.join(results_dir, f"journalctl_{host}.log")
                # journalctl --since "5 minutes ago"
                cmd = 'journalctl --since "5 minutes ago" --no-pager'
                output = executor.run_remote(host, cmd, check=False)
                with open(log_file, "w") as f:
                    f.write(output)
                print(f"[{host}] Logs collected.")
            except Exception as ex:
                print(f"[{host}] Failed to collect logs: {ex}")

        for host in hosts:
            t = threading.Thread(target=collect_host_logs, args=(host,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join(timeout=60)  # Wait up to 60 seconds per host (though they run in parallel)
            if t.is_alive():
                print(f"Warning: Thread for host log collection still alive after timeout.")

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
    def format_config_value(value):
        """Convert a config value to its string representation.
        Booleans are converted to 0/1; all other values are returned unchanged."""
        if isinstance(value, bool):
            return 1 if value else 0
        return value

    # $VAR or ${VAR} — same forms the shell expands in double quotes
    _ENV_VAR_RE = re.compile(
        r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)"
    )

    @staticmethod
    def expand_env_value(value, known_vars):
        """Expand $VAR / ${VAR} using known_vars, shell-style (single pass).

        Only names present in known_vars are substituted. Unknown references
        (e.g. ${PATH} on a remote host) are left unchanged so the shell can
        expand them at export time.
        """
        if value is None:
            return ""
        if not isinstance(value, str):
            value = str(value)

        def repl(match):
            name = match.group(1) or match.group(2)
            if name in known_vars:
                return str(known_vars[name])
            return match.group(0)

        return CommonUtils._ENV_VAR_RE.sub(repl, value)

    @staticmethod
    def expand_env_vars_map(env_vars):
        """Return a new dict with values expanded shell-style in declaration order.

        Each entry may reference earlier keys. Unknown references are left intact.
        """
        if not env_vars:
            return {}
        known = {}
        for k, v in env_vars.items():
            known[k] = CommonUtils.expand_env_value(v, known)
        return known

    @staticmethod
    def format_env_exports(env_vars):
        """Shell export statements for env_vars (values are double-quoted).

        Values are expanded shell-style in declaration order: each entry may
        reference earlier keys in env_vars (e.g. CEPH_INSTALL_PREFIX). References
        to variables not yet defined in env_vars are left intact for the remote
        shell (e.g. ${PATH}, ${LD_LIBRARY_PATH}).
        """
        if not env_vars:
            return ""
        expanded = CommonUtils.expand_env_vars_map(env_vars)
        return "".join(f'export {k}="{v}"; ' for k, v in expanded.items())

    @staticmethod
    def with_env_exports(cmd, env_vars, sudo=False):
        """Prefix cmd with env exports; optionally wrap under sudo bash -c.

        Both export values and the command string are expanded using env_vars
        (so paths like ``${CEPH_INSTALL_PREFIX}/bin/ceph`` resolve).
        """
        known = CommonUtils.expand_env_vars_map(env_vars)
        if cmd is not None:
            cmd = CommonUtils.expand_env_value(cmd, known)
        exports = "".join(f'export {k}="{v}"; ' for k, v in known.items()) if known else ""
        if sudo:
            escaped = cmd.replace("'", "'\\''")
            return f"sudo bash -c '{exports}{escaped}'"
        return f"{exports}{cmd}" if exports else cmd

    @staticmethod
    def mount_name_suffix(config=None, sep="_", settings=None):
        """Mount manager label after mds settings; empty for StubMountManager."""
        name = None
        if config is not None:
            name = getattr(config, "mount_display_name", None)
        if not name and isinstance(settings, dict):
            name = settings.get("mount_display_name")
        return f"{sep}{name}" if name else ""

    @staticmethod
    def get_short_name(var_name):
        """Map a human-readable parameter name to its short abbreviation."""
        name_map = {
            "MDS Cache Memory Limit": "m",
            "MDS Dispatch Engine": "de",
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
            "Ganesha Sync Data Only": "gsdo",
            "Ganesha Client Fsync To Rados": "gcftr",
            "Ganesha Async": "gas",
            "Ganesha Zero Copy": "gzc",
            "Ganesha Client Object Cache Size": "gocs",
            "Ganesha Msgr Workers": "gmw",
            "Ganesha Slot Table Size": "gsts",
            "Ganesha RPC IOQ Thread Min": "grpcmin",
            "Ganesha RPC IOQ Thread Max": "grpcmax",
            "Ganesha User ID": "guid",
            "Ganesha Keyring Path": "gkp",
            "Ganesha Ceph Binary Path": "gcbp",
            "Ganesha Enabled": "ge",
            "Samba Clustering": "scl",
            "Samba Workgroup": "swg",
            "Samba Ceph VFS": "scv",
            "Samba Client Object Cache Size": "socs",
            "Samba Msgr Workers": "smw",
            "Samba Client Log Level": "scll",
            "Samba Finisher Log Level": "sfll",
            "Samba Enabled": "se",
            "Samba Type": "st",
            "Samba User ID": "suid",
            "Samba Keyring Path": "skp",
            "Samba Ceph Binary Path": "scbp",
            "RGW Count Per Host": "rcph",
            "RGW Frontend Port": "rfp",
            "RGW Enabled": "re",
            "RGW Type": "rt",
            "Workload Runner": "wr",
            "Fio Threads": "ft",
            "Msgr Workers": "mw",
            "CephFS Tool Async": "cta",
            "CephFS Tool Queue Depth": "ctqd",
            "Fsync Every": "fe",
            "Min Object Size": "minos",
            "Max Object Size": "maxos",
        }
        return name_map.get(var_name, var_name.replace(" ", "_").replace("/", "_"))

    @staticmethod
    def get_samba_ceph_vfs_config_str(settings):
        """Encode vfs_ceph client params for result directory names."""
        if not settings.get("ceph_vfs"):
            return ""
        parts = []
        if settings.get("client_oc_size") is not None:
            size_str = CommonUtils.format_si_units(
                CommonUtils.parse_si_unit(settings["client_oc_size"])
            )
            parts.append(
                f"{CommonUtils.get_short_name('Samba Client Object Cache Size')}{size_str}"
            )
        if settings.get("msgr_workers") is not None:
            parts.append(
                f"{CommonUtils.get_short_name('Samba Msgr Workers')}{settings['msgr_workers']}"
            )
        return "_".join(parts)

    @staticmethod
    def get_samba_ceph_vfs_path_parts(config=None, settings=None):
        """Short-encoded path parts for loadpoint result filenames."""
        ceph_vfs = False
        oc_size = None
        msgr = None

        if config is not None:
            ceph_vfs = bool(
                getattr(config, "samba_enabled", False)
                and getattr(config, "samba_ceph_vfs", False)
            )
            oc_size = getattr(config, "samba_client_oc_size", None)
            msgr = getattr(config, "samba_msgr_workers", None)
        if settings:
            if settings.get("samba_ceph_vfs") or settings.get("ceph_vfs"):
                ceph_vfs = True
            if settings.get("samba_client_oc_size") is not None:
                oc_size = settings["samba_client_oc_size"]
            elif settings.get("client_oc_size") is not None:
                oc_size = settings["client_oc_size"]
            if settings.get("samba_msgr_workers") is not None:
                msgr = settings["samba_msgr_workers"]
            elif settings.get("msgr_workers") is not None:
                msgr = settings["msgr_workers"]

        if not ceph_vfs:
            return []

        parts = []
        if oc_size is not None:
            parts.append(
                f"{CommonUtils.get_short_name('Samba Client Object Cache Size')}"
                f"{CommonUtils.format_si_units(oc_size)}"
            )
        if msgr is not None:
            parts.append(
                f"{CommonUtils.get_short_name('Samba Msgr Workers')}{msgr}"
            )
        return parts

    @staticmethod
    def update_ceph_conf_section(text, section, options):
        """Merge *options* into a ceph.conf-style ``[section]``.

        Replaces existing keys in the section and appends the section when
        missing. Other sections are left unchanged.
        """
        if text is None:
            text = ""
        if not text.endswith("\n"):
            text += "\n"
        lines = text.splitlines(keepends=True)
        header = f"[{section}]"
        start = None
        end = len(lines)
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith("[") and stripped.endswith("]"):
                if stripped.lower() == header.lower():
                    start = i
                elif start is not None:
                    end = i
                    break
        option_lines = {k: f"    {k} = {v}\n" for k, v in options.items()}
        if start is None:
            new_section = [f"\n{header}\n"]
            for k in sorted(options):
                new_section.append(option_lines[k])
            return text + "".join(new_section)

        seen = set()
        i = start + 1
        while i < end:
            stripped = lines[i].strip()
            if not stripped or stripped.startswith("#"):
                i += 1
                continue
            if "=" in stripped:
                key = stripped.split("=", 1)[0].strip()
                if key in options:
                    lines[i] = option_lines[key]
                    seen.add(key)
            i += 1
        insert_at = end
        for k in sorted(options):
            if k not in seen:
                lines.insert(insert_at, option_lines[k])
                insert_at += 1
        return "".join(lines)

    @staticmethod
    def format_si_units(value):
        """Format an integer with SI/IEC unit suffixes when evenly divisible.

        Decimal prefixes are uppercase (``K``/``M``/``G``/…) because Ceph's
        ``strict_si_cast`` rejects lowercase (``k`` → "unit prefix not
        recognized"); the option is then ignored and stays at its default.
        """
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
            for unit in ["K", "M", "G", "T", "P"]:
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
            "mds_dispatch_engine": "MDS Dispatch Engine",
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
            "async": "CephFS Tool Async",
            "queue-depth": "CephFS Tool Queue Depth",
            "fsync-every": "Fsync Every",
            "ganesha_worker_threads": "Ganesha Worker Threads",
            "ganesha_umask": "Ganesha Umask",
            "ganesha_client_oc": "Ganesha Client Object Cache",
            "ganesha_syncdataonly": "Ganesha Sync Data Only",
            "ganesha_client_fsync_to_rados": "Ganesha Client Fsync To Rados",
            "ganesha_async": "Ganesha Async",
            "ganesha_zerocopy": "Ganesha Zero Copy",
            "ganesha_client_oc_size": "Ganesha Client Object Cache Size",
            "ganesha_msgr_workers": "Ganesha Msgr Workers",
            "ganesha_slot_table_size": "Ganesha Slot Table Size",
            "ganesha_rpc_ioq_thrdmin": "Ganesha RPC IOQ Thread Min",
            "ganesha_rpc_ioq_thrdmax": "Ganesha RPC IOQ Thread Max",
            "ganesha_user_id": "Ganesha User ID",
            "ganesha_keyring_path": "Ganesha Keyring Path",
            "ganesha_ceph_binary_path": "Ganesha Ceph Binary Path",
            "ganesha_enabled": "Ganesha Enabled",
            "samba_enabled": "Samba Enabled",
            "samba_type": "Samba Type",
            "samba_ceph_vfs": "Samba Ceph VFS",
            "samba_clustering": "Samba Clustering",
            "samba_workgroup": "Samba Workgroup",
            "samba_client_oc_size": "Samba Client Object Cache Size",
            "samba_msgr_workers": "Samba Msgr Workers",
            "samba_client_log_level": "Samba Client Log Level",
            "samba_finisher_log_level": "Samba Finisher Log Level",
            "samba_user_id": "Samba User ID",
            "samba_keyring_path": "Samba Keyring Path",
            "samba_ceph_binary_path": "Samba Ceph Binary Path",
            "threads_fio": "Fio Threads",
            "msgr_workers": "Msgr Workers",
            "min-object-size": "Min Object Size",
            "max-object-size": "Max Object Size",
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
            "ganesha_syncdataonly",
            "ganesha_client_fsync_to_rados",
            "ganesha_async",
            "ganesha_zerocopy",
            "ganesha_msgr_workers",
            "ganesha_slot_table_size",
            "ganesha_client_oc_size",
            "ganesha_rpc_ioq_thrdmin",
            "ganesha_rpc_ioq_thrdmax",
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

        samba_keys = [
            "samba_enabled",
            "samba_type",
            "samba_ceph_vfs",
            "samba_clustering",
            "samba_workgroup",
            "samba_client_oc_size",
            "samba_msgr_workers",
            "samba_client_log_level",
            "samba_finisher_log_level",
            "samba_user_id",
            "samba_keyring_path",
            "samba_ceph_binary_path",
        ]

        for k in samba_keys:
            if k in settings:
                name = name_map.get(k, k)
                params[name] = format_val(settings[k])

        if config and config.samba_enabled:
            for k in samba_keys:
                if k not in settings:
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
            "env_vars",
            "config_path",
            "keyring",
            "client_id",
            "client_log_level",
            "finisher_log_level",
            "log_level",
            "root_path",
            "duration",
            "ganesha_enabled",
            "ganesha_worker_threads",
            "ganesha_umask",
            "ganesha_client_oc",
            "ganesha_syncdataonly",
            "ganesha_client_fsync_to_rados",
            "ganesha_async",
            "ganesha_zerocopy",
            "ganesha_client_oc_size",
            "ganesha_msgr_workers",
            "ganesha_slot_table_size",
            "ganesha_rpc_ioq_thrdmin",
            "ganesha_rpc_ioq_thrdmax",
            "ganesha_user_id",
            "ganesha_keyring_path",
            "ganesha_ceph_binary_path",
            "samba_enabled",
            "samba_type",
            "samba_ceph_vfs",
            "samba_clustering",
            "samba_workgroup",
            "samba_client_oc_size",
            "samba_msgr_workers",
            "samba_client_log_level",
            "samba_finisher_log_level",
            "samba_user_id",
            "samba_keyring_path",
            "samba_ceph_binary_path",
            "workload_dir",
            "run_name",
            "num_filesystems",
            "mounts_per_fs",
            "mount_options",
            "perf_record",
            "perf_record_script",
            "perf_record_executable",
            "perf_record_duration",
            "lockstat",
            "threads_fio",
            "cephfs_tool_lockstat_enabled",
            "cephfs_tool_lockstat_asok",
            "cephfs_tool_lockstat_path",
            "timestamp_progress",
            "no_cleanup",
            "rbd_executable_path",
            "ramp_time",
            "pool",
            "recreate_images",
            "mount_display_name",
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
                g_parts.append(f"{CommonUtils.get_short_name('Ganesha Client Object Cache')}{CommonUtils.format_config_value(config.ganesha_client_oc)}")
            if config.ganesha_syncdataonly is not None:
                g_parts.append(f"{CommonUtils.get_short_name('Ganesha Sync Data Only')}{CommonUtils.format_config_value(config.ganesha_syncdataonly)}")
            if config.ganesha_client_fsync_to_rados is not None:
                g_parts.append(f"{CommonUtils.get_short_name('Ganesha Client Fsync To Rados')}{CommonUtils.format_config_value(config.ganesha_client_fsync_to_rados)}")
            if config.ganesha_async is not None:
                g_parts.append(f"{CommonUtils.get_short_name('Ganesha Async')}{CommonUtils.format_config_value(config.ganesha_async)}")
            if config.ganesha_zerocopy is not None:
                g_parts.append(f"{CommonUtils.get_short_name('Ganesha Zero Copy')}{CommonUtils.format_config_value(config.ganesha_zerocopy)}")
            if config.ganesha_client_oc_size:
                g_parts.append(
                    f"{CommonUtils.get_short_name('Ganesha Client Object Cache Size')}{CommonUtils.format_si_units(config.ganesha_client_oc_size)}"
                )
            if config.ganesha_msgr_workers:
                g_parts.append(f"{CommonUtils.get_short_name('Ganesha Msgr Workers')}{config.ganesha_msgr_workers}")
            if config.ganesha_slot_table_size:
                g_parts.append(f"{CommonUtils.get_short_name('Ganesha Slot Table Size')}{config.ganesha_slot_table_size}")
            if g_parts:
                g_p = "-" + "-".join(g_parts)
        elif settings.get("ganesha_enabled"):
            g_parts = []
            if settings.get("ganesha_worker_threads"):
                g_parts.append(f"{CommonUtils.get_short_name('Ganesha Worker Threads')}{settings['ganesha_worker_threads']}")
            if settings.get("ganesha_umask"):
                g_parts.append(f"{CommonUtils.get_short_name('Ganesha Umask')}{settings['ganesha_umask']}")
            if settings.get("ganesha_client_oc") is not None:
                g_parts.append(f"{CommonUtils.get_short_name('Ganesha Client Object Cache')}{CommonUtils.format_config_value(settings['ganesha_client_oc'])}")
            if settings.get("ganesha_syncdataonly") is not None:
                g_parts.append(f"{CommonUtils.get_short_name('Ganesha Sync Data Only')}{CommonUtils.format_config_value(settings['ganesha_syncdataonly'])}")
            if settings.get("ganesha_client_fsync_to_rados") is not None:
                g_parts.append(f"{CommonUtils.get_short_name('Ganesha Client Fsync To Rados')}{CommonUtils.format_config_value(settings['ganesha_client_fsync_to_rados'])}")
            if settings.get("ganesha_async") is not None:
                g_parts.append(f"{CommonUtils.get_short_name('Ganesha Async')}{CommonUtils.format_config_value(settings['ganesha_async'])}")
            if settings.get("ganesha_zerocopy") is not None:
                g_parts.append(f"{CommonUtils.get_short_name('Ganesha Zero Copy')}{CommonUtils.format_config_value(settings['ganesha_zerocopy'])}")
            if settings.get("ganesha_client_oc_size"):
                g_parts.append(
                    f"{CommonUtils.get_short_name('Ganesha Client Object Cache Size')}{CommonUtils.format_si_units(settings['ganesha_client_oc_size'])}"
                )
            if settings.get("ganesha_msgr_workers"):
                g_parts.append(f"{CommonUtils.get_short_name('Ganesha Msgr Workers')}{settings['ganesha_msgr_workers']}")
            if settings.get("ganesha_slot_table_size"):
                g_parts.append(f"{CommonUtils.get_short_name('Ganesha Slot Table Size')}{settings['ganesha_slot_table_size']}")
            if g_parts:
                g_p = "-" + "-".join(g_parts)

        s_p = ""
        s_parts = CommonUtils.get_samba_ceph_vfs_path_parts(
            config=config, settings=settings
        )
        if s_parts:
            s_p = "-" + "-".join(s_parts)

        lp_str = f"lp{int(lp):02d}" if lp is not None else "lp00"

        parts = []
        if lp_cfg:
            if "size" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('File Size')}{lp_cfg['size']}")
            if "threads" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('Threads')}{lp_cfg['threads']}")
            if "client-oc" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('Client Object Cache')}{CommonUtils.format_config_value(lp_cfg['client-oc'])}")
            if "client-oc-size" in lp_cfg:
                parts.append(
                    f"{CommonUtils.get_short_name('Client Object Cache Size')}{CommonUtils.format_si_units(lp_cfg['client-oc-size'])}"
                )
            if "block-size" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('Block Size')}{CommonUtils.format_si_units(lp_cfg['block-size'])}")
            if "min-object-size" in lp_cfg:
                parts.append(
                    f"{CommonUtils.get_short_name('Min Object Size')}{lp_cfg['min-object-size']}"
                )
            if "max-object-size" in lp_cfg:
                parts.append(
                    f"{CommonUtils.get_short_name('Max Object Size')}{lp_cfg['max-object-size']}"
                )
            if "iodepth" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('I/O Depth')}{lp_cfg['iodepth']}")
            if "readwrite" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('Read/Write Pattern')}{lp_cfg['readwrite']}")
            if "ioengine" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('I/O Engine')}{lp_cfg['ioengine']}")
            if "direct" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('Direct I/O')}{CommonUtils.format_config_value(lp_cfg['direct'])}")
            if "buffered" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('Buffered I/O')}{CommonUtils.format_config_value(lp_cfg['buffered'])}")
            if "create_serialize" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('Create Serialize')}{CommonUtils.format_config_value(lp_cfg['create_serialize'])}")
            if "msgr_workers" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('Msgr Workers')}{lp_cfg['msgr_workers']}")
            if "async" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('CephFS Tool Async')}{CommonUtils.format_config_value(lp_cfg['async'])}")
            if "queue-depth" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('CephFS Tool Queue Depth')}{lp_cfg['queue-depth']}")
            if "fsync-every" in lp_cfg:
                parts.append(f"{CommonUtils.get_short_name('Fsync Every')}{CommonUtils.format_config_value(lp_cfg['fsync-every'])}")
            # if "gtod_reduce" in lp_cfg:
            #     parts.append(f"gr{lp_cfg['gtod_reduce']}")
            # elif "gtod_reduce" in settings:
            #     parts.append(f"gr{settings['gtod_reduce']}")
            # if "ramp_time" in lp_cfg:
            #     parts.append(f"rt{lp_cfg['ramp_time']}")
            # elif "ramp_time" in settings:
            #     parts.append(f"rt{settings['ramp_time']}")

        options = mds_p + CommonUtils.mount_name_suffix(config, sep="-", settings=settings) + g_p + s_p
        if parts:
            options += f"_{'_'.join(parts)}"

        return f"{workload}_{output_type}_{client}_{lp_str}_{options}"

    @staticmethod
    def get_summary(data):
        """Compute a Summary section for a workload result JSON.

        The shape of the returned dict depends on ``test_parameters['Workload Runner']``:
          * fio:         {read:  {agg_bw_mib, agg_iops},
                          write: {agg_bw_mib, agg_iops},
                          agg_bw_mib, agg_iops,
                          read_bw_bytes, write_bw_bytes, read_iops, write_iops}
          * cephfs_tool: {read:  {agg_bw_mib, agg_iops},
                          write: {agg_bw_mib, agg_iops},
                          agg_bw_mib, agg_iops}
          * sfs2020:     {agg_bw_mib, agg_iops}
        """
        test_params = data.get("test_parameters", {}) or {}
        runner = test_params.get("Workload Runner", "fio")

        if runner in ("fio", "rbd"):
            # fio --ioengine=rbd emits the same JSON shape as regular fio.
            job = (data.get("jobs") or [{}])[0]
            read = job.get("read", {}) or {}
            write = job.get("write", {}) or {}

            read_runtime = read.get("runtime", 0)
            write_runtime = write.get("runtime", 0)
            max_runtime_ms = max(read_runtime, write_runtime)

            # Calculate per-direction metrics
            read_bw_mib = 0.0
            read_iops_val = 0.0
            if read_runtime > 0:
                read_bw_mib = (read.get("io_bytes", 0) / (read_runtime / 1000.0)) / (1024 * 1024)
                read_iops_val = read.get("total_ios", 0) / (read_runtime / 1000.0)

            write_bw_mib = 0.0
            write_iops_val = 0.0
            if write_runtime > 0:
                write_bw_mib = (write.get("io_bytes", 0) / (write_runtime / 1000.0)) / (1024 * 1024)
                write_iops_val = write.get("total_ios", 0) / (write_runtime / 1000.0)

            # Calculate aggregate metrics
            agg_bw_mib = 0.0
            agg_iops = 0.0
            if max_runtime_ms > 0:
                total_bytes = read.get("io_bytes", 0) + write.get("io_bytes", 0)
                agg_bw_mib = (total_bytes / (max_runtime_ms / 1000.0)) / (1024 * 1024)
                total_ios = read.get("total_ios", 0) + write.get("total_ios", 0)
                agg_iops = total_ios / (max_runtime_ms / 1000.0)

            return {
                "read": {
                    "agg_bw_mib": read_bw_mib,
                    "agg_iops": read_iops_val,
                },
                "write": {
                    "agg_bw_mib": write_bw_mib,
                    "agg_iops": write_iops_val,
                },
                "agg_bw_mib": agg_bw_mib,
                "agg_iops": agg_iops,
                # Keep legacy fields for backward compatibility
                "read_bw_bytes": read.get("bw_bytes", 0),
                "write_bw_bytes": write.get("bw_bytes", 0),
                "read_iops": read.get("iops", 0),
                "write_iops": write.get("iops", 0),
            }

        if runner == "cephfs_tool":
            summary = data.get("summary", {}) or {}
            read_bw = summary.get("Read Throughput", {}).get("mean", 0)
            read_iops = summary.get("File Reads (Opens)", {}).get("mean", 0)
            write_bw = summary.get("Write Throughput", {}).get("mean", 0)
            write_iops = summary.get("File Creates", {}).get("mean", 0)
            
            return {
                "read": {
                    "agg_bw_mib": read_bw,
                    "agg_iops": read_iops,
                },
                "write": {
                    "agg_bw_mib": write_bw,
                    "agg_iops": write_iops,
                },
                "agg_bw_mib": read_bw + write_bw,
                "agg_iops": read_iops + write_iops,
            }

        if runner == "sfs2020":
            runs = data.get("runs", []) or []
            if not runs:
                return {"agg_bw_mib": 0, "agg_iops": 0}
            metrics = runs[-1].get("metrics", {}) or {}
            throughput = metrics.get("throughput", {}).get("value", 0) or 0
            units = (metrics.get("throughput", {}).get("units") or "").lower()
            if "kib/s" in units:
                throughput /= 1024.0
            iops = metrics.get("ops/s", {}).get("value", 0) or 0
            if not iops:
                iops = metrics.get("iops", {}).get("value", 0) or 0
            return {"agg_bw_mib": throughput, "agg_iops": iops}

        if runner == "rados_bench":
            def _to_float(v):
                try:
                    return float(v)
                except (TypeError, ValueError):
                    return 0.0

            bw_mib = _to_float(data.get("bandwidth"))
            iops = _to_float(data.get("average_iops"))
            pattern = str(test_params.get("Read/Write Pattern", "")).lower()

            read_bw, read_iops, write_bw, write_iops = 0.0, 0.0, 0.0, 0.0
            if "read" in pattern:
                read_bw, read_iops = bw_mib, iops
            elif "write" in pattern:
                write_bw, write_iops = bw_mib, iops

            return {
                "read": {
                    "agg_bw_mib": read_bw,
                    "agg_iops": read_iops,
                },
                "write": {
                    "agg_bw_mib": write_bw,
                    "agg_iops": write_iops,
                },
                "agg_bw_mib": read_bw + write_bw,
                "agg_iops": read_iops + write_iops,
            }

        return {}

    @staticmethod
    def aggregate_results_summaries(summaries):
        """Sum bandwidth and IOPS fields across multiple test_results_summary dicts."""

        def merge(a, b):
            if not a:
                return dict(b) if b else {}
            if not b:
                return dict(a)
            merged = {}
            for key in set(a.keys()) | set(b.keys()):
                va = a.get(key)
                vb = b.get(key)
                if isinstance(va, dict) or isinstance(vb, dict):
                    merged[key] = merge(va or {}, vb or {})
                elif isinstance(va, (int, float)) and isinstance(vb, (int, float)):
                    merged[key] = va + vb
                elif va is not None:
                    merged[key] = va
                else:
                    merged[key] = vb
            return merged

        aggregated = {}
        for summary in summaries:
            if summary:
                aggregated = merge(aggregated, summary)
        return aggregated

    @staticmethod
    def write_multi_client_results_summary(
        workload,
        client_results,
        results_dir,
        loadpoint,
        settings,
        lp_cfg=None,
        num_clients=None,
        config=None,
    ):
        """Write an aggregated results summary JSON for multi-client workloads."""
        if num_clients is not None and num_clients <= 1:
            return None
        if not client_results:
            return None

        summaries = [
            result.get("test_results_summary", {})
            for result in client_results
            if result.get("test_results_summary") is not None
        ]
        if not summaries:
            return None

        test_params = client_results[0].get("test_parameters", {})
        summary_data = {
            "test_parameters": test_params,
            "test_results_summary": CommonUtils.aggregate_results_summaries(
                summaries
            ),
        }

        filename = (
            f"{CommonUtils.get_workload_base_name(workload, 'results_summary', 'all', loadpoint, settings, lp_cfg, config)}.json"
        )
        output_path = os.path.join(results_dir, filename)
        with open(output_path, "w") as f:
            json.dump(summary_data, f, indent=4)
        print(f"Wrote multi-client results summary to {output_path}")
        return output_path
