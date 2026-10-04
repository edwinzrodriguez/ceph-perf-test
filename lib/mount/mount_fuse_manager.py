import os
import re
import shlex
import time

from cephfs_perf_lib import CommonUtils
from lib.mount.mount_manager import MountManager
from lib.run_logging import get_logger

log = get_logger(__name__)



class MountFuseManager(MountManager):
    """Mount CephFS via ``ceph-fuse`` on client hosts.

    Used when ``mount_manager_type`` is ``MountFuseManager`` and Ganesha is
    disabled. Monitor addresses are resolved on the admin host (same as the
    kernel client). Auth and config paths default to the top-level ``ceph:``
    section; override via ``mount_fuse`` in the settings YAML.

    A dedicated per-mount Ceph conf is generated (like Ganesha's
    ``ganesha-ceph-<host>.conf``) so each ``ceph-fuse`` process gets a
    predictable admin socket for perf dumps and optional client logging.
    """

    _LOGGING_RESERVED_KEYS = frozenset({"enabled", "memory"})

    def __init__(self, executor, config, fs_manager):
        super().__init__(executor, config, fs_manager)
        self.fs_manager = fs_manager
        self._mounts = []

    def _fuse_cfg(self):
        return self.config.get("mount_fuse", {}) or {}

    def _fuse_bin(self):
        fuse_cfg = self._fuse_cfg()
        if fuse_cfg.get("binary_path"):
            return self.config.expand_env(fuse_cfg["binary_path"])
        known = CommonUtils.expand_env_vars_map(self.config.env_vars)
        prefix = known.get("CEPH_INSTALL_PREFIX") or "/usr/local"
        return f"{prefix}/bin/ceph-fuse"

    def _ceph_bin(self):
        return self.config.resolve_ceph_binary("mount_fuse")

    def _fuse_env(self):
        return self.config.get_merged_env_vars(self._fuse_cfg().get("env_vars"))

    def _cluster_conf(self):
        return self._fuse_cfg().get("conf") or self.config.ceph_conf_path

    def _keyring(self):
        return self._fuse_cfg().get("keyring") or self.config.ceph_keyring_path

    def _client_id(self):
        return self._fuse_cfg().get("client_id") or self.config.ceph_user_id

    def _run_dir(self):
        return self._fuse_cfg().get("run_dir", "/var/run/ceph")

    def _log_dir(self):
        return self._fuse_cfg().get("log_dir", "/var/log/ceph")

    def _logging_cfg(self):
        return self._fuse_cfg().get("logging") or {}

    def is_logging_enabled(self):
        return bool(self._logging_cfg().get("enabled"))

    @staticmethod
    def _safe_host(host_name):
        return host_name.replace(".", "-")

    def _instance_parts(self, host_name, fs, index, mpfs):
        parts = [self._safe_host(host_name)]
        if len(self.fs_names) > 1:
            parts.append(fs)
        if mpfs > 1:
            parts.append(f"{index:02d}")
        return parts

    def _format_path(self, template, host_name, fs, index, mpfs):
        safe = self._safe_host(host_name)
        path = template.format(host=safe, fs=fs, index=f"{index:02d}")
        extra = []
        if len(self.fs_names) > 1 and "{fs}" not in template:
            extra.append(fs)
        if mpfs > 1 and "{index}" not in template:
            extra.append(f"{index:02d}")
        if extra:
            root, ext = os.path.splitext(path)
            path = f"{root}-{'-'.join(extra)}{ext}"
        return path

    def _asok_path(self, host_name, fs, index, mpfs):
        template = self._fuse_cfg().get(
            "asok", f"{self._run_dir()}/ceph-fuse-{{host}}.asok"
        )
        return self._format_path(template, host_name, fs, index, mpfs)

    def _generated_conf_path(self, host_name, fs, index, mpfs):
        template = self._fuse_cfg().get(
            "conf_path", "/etc/ceph/ceph-fuse-{host}.conf"
        )
        return self._format_path(template, host_name, fs, index, mpfs)

    def _log_path(self, host_name, fs, index, mpfs):
        template = self._fuse_cfg().get(
            "log_file", f"{self._log_dir()}/ceph-fuse-{{host}}.log"
        )
        return self._format_path(template, host_name, fs, index, mpfs)

    def _mount_label(self, host_name, fs, index, mpfs):
        return "-".join(self._instance_parts(host_name, fs, index, mpfs))

    def _logging_conf_options(self, log_path):
        """Return client-section options from ``mount_fuse.logging``."""
        cfg = self._logging_cfg()
        options = {}
        if self.is_logging_enabled():
            options["log_file"] = log_path
            options["log_to_file"] = "true"
            options["log_to_stderr"] = "false"
            options["log_to_syslog"] = "false"
            for key, val in cfg.items():
                if key in self._LOGGING_RESERVED_KEYS:
                    continue
                if isinstance(val, bool):
                    options[key] = "true" if val else "false"
                else:
                    options[key] = val
        else:
            options["log_to_file"] = "false"
            options["log_to_stderr"] = "false"
            options["log_to_syslog"] = "false"
        return options

    def _sudo_with_fuse_env(self, cmd):
        return CommonUtils.with_env_exports(cmd, self._fuse_env(), sudo=True)

    def _ceph_cli_args(self, include_keyring=True):
        args = []
        cluster_conf = self._cluster_conf()
        if cluster_conf:
            args.append(f"-c {shlex.quote(cluster_conf)}")
        client_id = self._client_id()
        if client_id:
            args.append(f"--id {shlex.quote(client_id)}")
        keyring = self._keyring()
        if include_keyring and keyring:
            args.append(f"--keyring {shlex.quote(keyring)}")
        return " ".join(args)

    def _admin_daemon_cmd(self, asok, args):
        """``ceph --admin-daemon`` using a short relative socket path."""
        asok = (asok or "").rstrip("/")
        if "/" in asok:
            directory, name = asok.rsplit("/", 1)
        else:
            directory, name = ".", asok
        inner = (
            f"cd {shlex.quote(directory)} && "
            f"{shlex.quote(self._ceph_bin())} --admin-daemon {shlex.quote(name)} {args}"
        )
        return self._sudo_with_fuse_env(inner)

    def _asok_exists(self, host_name, asok_path):
        out = self.executor.run_remote(
            host_name, f"test -S {shlex.quote(asok_path)} && echo OK || echo MISSING"
        )
        return "OK" in (out or "")

    def _wait_for_asok(self, host_name, asok_path):
        log.info(f"[{host_name}] Waiting for ceph-fuse admin socket {asok_path}...")
        for i in range(30):
            if self._asok_exists(host_name, asok_path):
                log.info(f"[{host_name}] ceph-fuse admin socket {asok_path} is available.")
                return
            if i == 29:
                log.info(
                    f"[{host_name}] Warning: ceph-fuse admin socket {asok_path} "
                    "NOT found after 30 seconds."
                )
            time.sleep(1)

    def _generate_minimal_conf(self, host_name):
        ceph_bin = self._ceph_bin()
        cmd = self._sudo_with_fuse_env(
            f"{shlex.quote(ceph_bin)} {self._ceph_cli_args()} config generate-minimal-conf"
        )
        try:
            text = self.executor.run_remote(host_name, cmd)
            if text and text.strip():
                return text
        except Exception as e:
            log.info(
                f"[{host_name}] Warning: generate-minimal-conf failed ({e}); "
                "falling back to cluster ceph.conf"
            )
        cluster_conf = self._cluster_conf()
        return self.executor.run_remote(
            host_name, f"sudo cat {shlex.quote(cluster_conf)} 2>/dev/null || true"
        )

    def _write_remote_file(self, host_name, path, content):
        escaped = content.replace("'", "'\\''")
        self.executor.run_remote(
            host_name,
            f"printf '%s' '{escaped}' | sudo tee {shlex.quote(path)} > /dev/null",
            check=True,
        )
        self.executor.run_remote(host_name, f"sudo chmod 0644 {shlex.quote(path)}")

    def _write_fuse_conf(self, host_name, conf_path, asok_path, log_path):
        """Write a dedicated ceph-fuse conf with asok and optional logging."""
        self.executor.run_remote(
            host_name,
            "sudo mkdir -p /etc/ceph /var/run/ceph /var/log/ceph",
        )
        self.executor.run_remote(
            host_name, "sudo chmod 0755 /var/run/ceph /var/log/ceph"
        )

        minimal = self._generate_minimal_conf(host_name)
        client_id = self._client_id()
        options = {
            "admin_socket": asok_path,
        }
        keyring = self._keyring()
        if keyring:
            options["keyring"] = keyring
        options.update(self._logging_conf_options(log_path))
        text = CommonUtils.update_ceph_conf_section(
            minimal, f"client.{client_id}", options
        )
        self._write_remote_file(host_name, conf_path, text)
        log.info(
            f"[{host_name}] Wrote ceph-fuse conf {conf_path} "
            f"(admin_socket={asok_path})"
        )

    def mount(self):
        admin_host = self.config.admin_host
        mon_dump = self.fs_manager._run_ceph(admin_host, "mon dump")
        mon_addrs = re.findall(
            r"v1:([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+)", mon_dump
        )
        if not mon_addrs:
            raise RuntimeError(
                f"Could not parse mon v1 address from `ceph mon dump` on {admin_host}"
            )
        maddrs = ",".join(dict.fromkeys(mon_addrs))

        fuse_bin = self._fuse_bin()
        client_id = self._client_id()
        extra_opts = (self._fuse_cfg().get("mount_options") or "").strip()
        env_vars = self._fuse_env()
        mpfs = self._mounts_per_fs()
        self._mounts = []

        for fs in self.fs_names:
            for c in self.clients:
                for i in range(mpfs):
                    p = f"/mnt/cephfs_{fs}" + (f"_{i:02d}" if mpfs > 1 else "")
                    asok_path = self._asok_path(c, fs, i, mpfs)
                    conf_path = self._generated_conf_path(c, fs, i, mpfs)
                    log_path = self._log_path(c, fs, i, mpfs)
                    self.executor.run_remote(
                        c, f"sudo rm -f {shlex.quote(asok_path)} || true"
                    )
                    self._write_fuse_conf(c, conf_path, asok_path, log_path)
                    fuse_cmd = (
                        f"{shlex.quote(fuse_bin)} {shlex.quote(p)} "
                        f"-m {shlex.quote(maddrs)} "
                        f"-c {shlex.quote(conf_path)} "
                        f"--id {shlex.quote(client_id)} "
                    )
                    keyring = self._keyring()
                    if keyring:
                        fuse_cmd += f"-k {shlex.quote(keyring)} "
                    fuse_cmd += f"--client_mds_namespace={shlex.quote(fs)}"
                    if extra_opts:
                        fuse_cmd = f"{fuse_cmd} {extra_opts}"
                    inner = (
                        f"mkdir -p {shlex.quote(p)} && "
                        f"{fuse_cmd} && "
                        f"mountpoint -q {shlex.quote(p)}"
                    )
                    self.executor.run_remote(
                        c,
                        CommonUtils.with_env_exports(inner, env_vars, sudo=True),
                        check=True,
                    )
                    u, _, _ = self.executor.get_ssh_details(c)
                    self.executor.run_remote(
                        c, f"sudo chown {u}:{u} {p}", check=True
                    )
                    self._wait_for_asok(c, asok_path)
                    self._mounts.append(
                        {
                            "host": c,
                            "fs": fs,
                            "index": i,
                            "mountpoint": p,
                            "asok": asok_path,
                            "conf": conf_path,
                            "log": log_path,
                            "label": self._mount_label(c, fs, i, mpfs),
                        }
                    )
                    log.info("[%s] ceph-fuse mounted %s at %s via %s", c, fs, p, maddrs)

    def unmount_clients(self):
        super().unmount_clients()
        for mount in self._mounts:
            host_name = mount["host"]
            asok_path = mount["asok"]
            self.executor.run_remote(
                host_name, f"sudo rm -f {shlex.quote(asok_path)} || true"
            )
        self._mounts = []

    def display_name(self):
        return "fuse"

    def fuse_mount(self):
        self.mount()

    def reset_perf_counters(self):
        """Reset ceph-fuse admin-socket perf counters (start of a loadpoint)."""
        if not self._mounts:
            return
        found = False
        for mount in self._mounts:
            host_name = mount["host"]
            asok_path = mount["asok"]
            if not self._asok_exists(host_name, asok_path):
                log.info(
                    f"[{host_name}] Warning: ceph-fuse admin socket not found "
                    f"({asok_path}); skip perf reset"
                )
                continue
            found = True
            log.info(
                f"[{host_name}] Resetting ceph-fuse perf counters via {asok_path}..."
            )
            self.executor.run_remote(
                host_name,
                self._admin_daemon_cmd(asok_path, "perf reset all"),
            )
        if not found:
            log.info("Warning: no ceph-fuse admin sockets found for perf counter reset")

    def _dump_dest_file(self, mount, loadpoint, settings, lp_cfg):
        label = mount["label"]
        if settings is not None:
            return (
                f"{CommonUtils.get_workload_base_name('ceph_fuse', 'perf_dump', label, loadpoint, settings, lp_cfg)}.json"
            )
        lp_tag = f"{int(loadpoint):02d}"
        return f"ceph_fuse_perf_dump_{label}_lp{lp_tag}.json"

    def _copy_asok_output_to_results(
        self, host, asok, results_subdir, admin_subcommand, dest_filename
    ):
        temp_file = f"/tmp/{dest_filename}"
        dump_cmd = self._admin_daemon_cmd(asok, admin_subcommand)
        self.executor.run_remote(
            host, f"{dump_cmd} | sudo tee {shlex.quote(temp_file)} > /dev/null"
        )
        user, _, _ = self.executor.get_ssh_details(host)
        self.executor.run_remote(
            host, f"sudo chown {user}:{user} {shlex.quote(temp_file)}"
        )
        admin_user, admin_host_addr, admin_port = self.executor.get_ssh_details(
            self.config.admin_host
        )
        copy_cmd = (
            f"scp -o StrictHostKeyChecking=no -P {admin_port} "
            f"{shlex.quote(temp_file)} {admin_user}@{admin_host_addr}:{results_subdir}/"
        )
        self.executor.run_remote(host, copy_cmd)
        self.executor.run_remote(host, f"rm -f {shlex.quote(temp_file)}")

    def dump_perf_counters(
        self, loadpoint, results_dir=None, settings=None, lp_cfg=None
    ):
        """Dump ceph-fuse admin-socket perf counters for the loadpoint window."""
        if not self._mounts:
            return
        if not results_dir:
            log.info(
                f"Skipping ceph-fuse perf dump for load point {loadpoint}: no results_dir"
            )
            return
        found = False
        for mount in self._mounts:
            host_name = mount["host"]
            asok_path = mount["asok"]
            if not self._asok_exists(host_name, asok_path):
                log.info(
                    f"[{host_name}] Warning: ceph-fuse admin socket not found "
                    f"({asok_path}); skip perf dump"
                )
                continue
            found = True
            dest_file = self._dump_dest_file(mount, loadpoint, settings, lp_cfg)
            log.info(
                f"[{host_name}] Dumping ceph-fuse perf counters "
                f"(Load Point {loadpoint}) via {asok_path}..."
            )
            self._copy_asok_output_to_results(
                host_name, asok_path, results_dir, "perf dump", dest_file
            )
            log.info(f"[{host_name}] Wrote ceph-fuse perf dump {dest_file} → {results_dir}/")
        if not found:
            log.info(
                f"Warning: no ceph-fuse admin sockets found for perf dump "
                f"(Load Point {loadpoint})"
            )

    def collect_client_logs(self, loadpoint, results_dir=None):
        if not self.is_logging_enabled() or not results_dir:
            return
        lp_tag = f"{int(loadpoint):02d}"
        for mount in self._mounts:
            host_name = mount["host"]
            src_log = mount["log"]
            check = self.executor.run_remote(
                host_name,
                f"test -f {shlex.quote(src_log)} && echo EXISTS || echo MISSING",
            ).strip()
            if "EXISTS" not in check:
                log.info(
                    f"[{host_name}] Warning: ceph-fuse log {src_log} not found; skipping"
                )
                continue
            dest_log = f"ceph_fuse_{mount['label']}_lp{lp_tag}.log"
            log.info(
                f"[{host_name}] Collecting ceph-fuse log to {results_dir}/{dest_log}..."
            )
            self.fs_manager._copy_log_to_results(
                host_name, src_log, dest_log, results_dir
            )
