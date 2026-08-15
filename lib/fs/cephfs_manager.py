import json
import re
import shlex
import time
from cephfs_perf_lib import CommonUtils, FSManager


class CephFSManager(FSManager):
    """Base CephFS manager with common lifecycle, logging, and lockstat logic.

    Deployment-specific behavior (cephadm orch vs local ceph-mds process) is
    implemented by subclasses via ``_remove_mds_service``, ``_deploy_mds``,
    and ``_collect_mds_logs``.
    """

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
        # Track deployed MDS instances: {fs_name: [(host, mds_id), ...]}
        self._mds_instances = {}

    # ------------------------------------------------------------------
    # ceph CLI helpers (always honor env_vars + sudo library path)
    # ------------------------------------------------------------------

    def _ceph_env(self):
        """Env for ceph CLI / MDS tools (top-level env_vars)."""
        return self.config.env_vars

    def _ceph_bin(self):
        """Resolved path to the ceph CLI.

        Preference order:
          1. mds.ceph_binary_path
          2. ganesha.ceph_binary_path
          3. ${CEPH_INSTALL_PREFIX}/bin/ceph  (from env_vars)
          4. /usr/local/bin/ceph
        """
        mds_cfg = self.config.get("mds", {}) or {}
        if mds_cfg.get("ceph_binary_path"):
            return self.config.expand_env(mds_cfg["ceph_binary_path"])
        ganesha_cfg = self.config.get("ganesha", {}) or {}
        if ganesha_cfg.get("ceph_binary_path"):
            return self.config.expand_env(ganesha_cfg["ceph_binary_path"])
        known = CommonUtils.expand_env_vars_map(self._ceph_env())
        prefix = known.get("CEPH_INSTALL_PREFIX") or "/usr/local"
        return f"{prefix}/bin/ceph"

    def _ceph_cmd(self, args, sudo=True):
        """Build a shell command that runs the ceph CLI with env_vars applied.

        ``args`` is the remainder of the command (e.g. ``"fs ls --format json"``).
        Paths and env references in env_vars are expanded; LD_LIBRARY_PATH is
        exported so a prefix-installed ``ceph`` can load its shared libs.
        """
        cmd = f"{self._ceph_bin()} {args}".strip()
        return CommonUtils.with_env_exports(cmd, self._ceph_env(), sudo=sudo)

    def _run_ceph(self, host, args, check=False, sudo=True):
        """Run the ceph CLI on *host* with env_vars + optional sudo."""
        return self.executor.run_remote(
            host, self._ceph_cmd(args, sudo=sudo), check=check
        )

    def _admin_daemon_cmd(self, asok, args):
        """``ceph --admin-daemon`` using a short relative socket path.

        Linux AF_UNIX ``sun_path`` is 108 bytes including the trailing NUL
        (107 usable). Cephadm sockets under ``/var/run/ceph/<fsid>/`` plus a
        long MDS id often exceed that (``AF_UNIX path too long``). ``cd`` to
        the socket directory and pass only the basename so connect() stays
        under the limit.
        """
        asok = (asok or "").rstrip("/")
        if "/" in asok:
            directory, name = asok.rsplit("/", 1)
        else:
            directory, name = ".", asok
        inner = (
            f"cd {shlex.quote(directory)} && "
            f"{self._ceph_bin()} --admin-daemon {shlex.quote(name)} {args}"
        )
        return CommonUtils.with_env_exports(inner, self._ceph_env(), sudo=True)

    def _mds_logging_cfg(self):
        """Return the MDS logging block, preferring ``mds.logging``.

        Falls back to a top-level ``logging`` section so older settings
        files keep working until they are migrated.
        """
        mds_cfg = self.config.get("mds", {}) or {}
        if "logging" in mds_cfg:
            return mds_cfg.get("logging") or {}
        return self.config.get("logging", {}) or {}

    def start_fs_logging(self, loadpoint):
        logging_cfg = self._mds_logging_cfg()
        debug_mds = logging_cfg.get("debug_mds", 20)
        debug_ms = logging_cfg.get("debug_ms", 1)
        for server_name in self.mdss:
            print(
                f"[{server_name}] Starting MDS debug logging for Load Point {loadpoint}"
            )
            self._run_ceph(server_name, f"config set mds debug_mds {debug_mds}")
            self._run_ceph(server_name, f"config set mds debug_ms {debug_ms}")

    def stop_fs_logging(self, loadpoint, results_dir=None):
        for server_name in self.mdss:
            print(
                f"[{server_name}] Stopping MDS debug logging for Load Point {loadpoint}"
            )
            self._run_ceph(server_name, "config set mds debug_mds 1")
            self._run_ceph(server_name, "config set mds debug_ms 1")
        if results_dir:
            self._collect_mds_logs(loadpoint, results_dir)

    def start_lockstat(self, fs):
        lockstat_cfg = self.config.get("specstorage", {}).get("lockstat", {})
        lockstat_path = self.config.expand_env(
            lockstat_cfg.get("path", "/usr/local/bin/ceph-lockstat")
        )
        threshold = lockstat_cfg.get("threshold", 0)
        env_vars = self._ceph_env()
        for server_name in self.mdss:
            if server_name not in self.lockstat_exists:
                check = self.executor.run_remote(
                    server_name,
                    f"test -f {lockstat_path} && echo EXISTS || echo MISSING",
                ).strip()
                self.lockstat_exists[server_name] = "EXISTS" in check
            if self.lockstat_exists[server_name]:
                print(
                    f"[{server_name}] Starting lockstat for mds.{fs} via {lockstat_path} with threshold {threshold}"
                )
                self.executor.run_remote(
                    server_name,
                    CommonUtils.with_env_exports(
                        f"python3 {lockstat_path} mds.{fs} start --threshold {threshold}",
                        env_vars,
                        sudo=True,
                    ),
                )

    def stop_lockstat(self, fs):
        lockstat_cfg = self.config.get("specstorage", {}).get("lockstat", {})
        lockstat_path = self.config.expand_env(
            lockstat_cfg.get("path", "/usr/local/bin/ceph-lockstat")
        )
        env_vars = self._ceph_env()
        for server_name in self.mdss:
            if self.lockstat_exists.get(server_name):
                print(f"[{server_name}] Stopping lockstat for mds.{fs} via {lockstat_path}")
                self.executor.run_remote(
                    server_name,
                    CommonUtils.with_env_exports(
                        f"python3 {lockstat_path} mds.{fs} stop",
                        env_vars,
                        sudo=True,
                    ),
                )

    def _check_lockstat_exists(self, server_name, lockstat_path):
        """Populate lockstat_exists for server_name if not yet checked."""
        if server_name not in self.lockstat_exists:
            check = self.executor.run_remote(
                server_name,
                f"test -f {lockstat_path} && echo EXISTS || echo MISSING",
            ).strip()
            self.lockstat_exists[server_name] = "EXISTS" in check

    def reset_lockstat(self, config_section="specstorage"):
        lockstat_cfg = self.config.get(config_section, {}).get("lockstat", {})
        lockstat_path = self.config.expand_env(
            lockstat_cfg.get("path", "/usr/local/bin/ceph-lockstat")
        )
        env_vars = self._ceph_env()
        for fs in self.get_fs_names():
            for server_name in self.mdss:
                self._check_lockstat_exists(server_name, lockstat_path)
                if self.lockstat_exists.get(server_name):
                    print(f"[{server_name}] Resetting lockstat for mds.{fs}")
                    self.executor.run_remote(
                        server_name,
                        CommonUtils.with_env_exports(
                            f"python3 {lockstat_path} mds.{fs} reset",
                            env_vars,
                            sudo=True,
                        ),
                    )

    def dump_lockstat(self, loadpoint, results_dir=None, phase=None, settings=None, lp_cfg=None, config_section="specstorage"):
        lockstat_cfg = self.config.get(config_section, {}).get("lockstat", {})
        lockstat_path = self.config.expand_env(
            lockstat_cfg.get("path", "/usr/local/bin/ceph-lockstat")
        )
        env_vars = self._ceph_env()
        for fs in self.get_fs_names():
            for server_name in self.mdss:
                self._check_lockstat_exists(server_name, lockstat_path)
                if self.lockstat_exists.get(server_name):
                    phase_label = f" ({phase} phase)" if phase else ""
                    print(
                        f"[{server_name}] Dumping lockstat for mds.{fs} (Load Point {loadpoint}{phase_label})"
                    )
                    if results_dir:
                        dump_cmd = CommonUtils.with_env_exports(
                            f"python3 {lockstat_path} mds.{fs} dump --detail",
                            env_vars,
                        )
                        CommonUtils.dump_lockstat_common(
                            self.executor,
                            server_name,
                            loadpoint,
                            results_dir,
                            f"mds.{fs}",
                            dump_cmd,
                            self.admin,
                            settings=settings,
                            lp_cfg=lp_cfg,
                            phase=phase,
                        )

    # ------------------------------------------------------------------
    # Admin-socket perf counters (full loadpoint window)
    # ------------------------------------------------------------------

    def _mds_asok_path(self, mds_id):
        """Default host path for an MDS admin socket (systemd layout)."""
        run_dir = (self.config.get("mds", {}) or {}).get("run_dir", "/var/run/ceph")
        return f"{run_dir}/ceph-mds.{mds_id}.asok"

    def _iter_mds_admin_sockets(self):
        """Yield ``(host, mds_id, asok_path)`` for known or discovered MDS daemons.

        Prefers constructed paths from ``_mds_instances`` when at least one of
        those sockets exists (local/systemd MDS). Otherwise lists
        ``ceph-mds.*.asok`` under the MDS run dir so cephadm sockets under
        ``/var/run/ceph/<fsid>/`` are found — without also picking up leftover
        sockets from a previous deployment on the same host.
        """
        seen = set()
        known = []
        for _fs, instances in (self._mds_instances or {}).items():
            for host, mds_id in instances:
                asok = self._mds_asok_path(mds_id)
                key = (host, asok)
                if key in seen:
                    continue
                seen.add(key)
                known.append((host, mds_id, asok))

        # systemd/local MDS writes a predictable asok. Do not also scrape
        # leftover cephadm sockets (often AF_UNIX-too-long) on that host.
        if known and any(self._asok_exists(h, a) for h, _, a in known):
            for item in known:
                yield item
            return

        for item in known:
            yield item

        run_dir = (self.config.get("mds", {}) or {}).get("run_dir", "/var/run/ceph")
        for host in self.mdss:
            listing = self.executor.run_remote(
                host,
                f"find {run_dir} -type s -name 'ceph-mds.*.asok' 2>/dev/null || true",
            ).strip()
            for asok in listing.split():
                if not asok or (host, asok) in seen:
                    continue
                base = asok.rsplit("/", 1)[-1]
                mds_id = base[len("ceph-mds.") : -len(".asok")] if base.startswith(
                    "ceph-mds."
                ) and base.endswith(".asok") else base
                seen.add((host, asok))
                yield host, mds_id, asok

    def _asok_exists(self, host, asok_path):
        out = self.executor.run_remote(
            host, f"test -S {asok_path} && echo OK || echo MISSING"
        ).strip()
        return "OK" in out

    def reset_perf_counters(self):
        """Reset MDS admin-socket perf counters (start of a loadpoint window).

        Mirrors GaneshaManager.reset_ganesha_perf / lockstat reset: call at the
        beginning of each RUN phase so the subsequent dump spans the whole
        loadpoint, not just a short ``perf record`` sample.
        """
        found = False
        for host, mds_id, asok in self._iter_mds_admin_sockets():
            if not self._asok_exists(host, asok):
                print(
                    f"[{host}] Warning: MDS admin socket not found for "
                    f"mds.{mds_id} ({asok}); skip perf reset"
                )
                continue
            found = True
            print(
                f"[{host}] Resetting MDS perf counters for mds.{mds_id} via {asok}..."
            )
            self.executor.run_remote(
                host,
                self._admin_daemon_cmd(asok, "perf reset all"),
            )
        if not found:
            print("Warning: no MDS admin sockets found for perf counter reset")

    def dump_perf_counters(
        self, loadpoint, results_dir=None, phase=None, settings=None, lp_cfg=None
    ):
        """Dump MDS admin-socket ``perf dump`` JSON for the loadpoint window.

        Mirrors dump_lockstat / collect_ganesha_perf_dump: call at loadpoint end
        so counters cover the full RUN phase.
        """
        if not results_dir:
            print(
                f"Skipping MDS perf dump for load point {loadpoint}: no results_dir"
            )
            return

        found = False
        for host, mds_id, asok in self._iter_mds_admin_sockets():
            if not self._asok_exists(host, asok):
                print(
                    f"[{host}] Warning: MDS admin socket not found for "
                    f"mds.{mds_id} ({asok}); skip perf dump"
                )
                continue
            found = True
            phase_label = f" ({phase} phase)" if phase else ""
            print(
                f"[{host}] Dumping MDS perf counters for mds.{mds_id} "
                f"(Load Point {loadpoint}{phase_label}) via {asok}..."
            )
            target_name = f"mds.{mds_id}"
            output_type = f"perf_dump_{phase}" if phase else "perf_dump"
            if settings is not None:
                dest_file = (
                    f"{CommonUtils.get_workload_base_name(target_name, output_type, host, loadpoint, settings, lp_cfg)}.json"
                )
            else:
                lp_tag = f"{int(loadpoint):02d}"
                phase_suffix = f"_{phase}" if phase else ""
                dest_file = (
                    f"{target_name}_perf_dump{phase_suffix}_{host}_lp{lp_tag}.json"
                )
            temp_file = f"/tmp/{dest_file}"
            dump_cmd = self._admin_daemon_cmd(asok, "perf dump")
            self.executor.run_remote(
                host, f"{dump_cmd} | sudo tee {temp_file} > /dev/null"
            )
            user, _, _ = self.executor.get_ssh_details(host)
            self.executor.run_remote(host, f"sudo chown {user}:{user} {temp_file}")
            admin_user, admin_host_addr, admin_port = self.executor.get_ssh_details(
                self.admin
            )
            copy_cmd = (
                f"scp -o StrictHostKeyChecking=no -P {admin_port} "
                f"{temp_file} {admin_user}@{admin_host_addr}:{results_dir}/"
            )
            self.executor.run_remote(host, copy_cmd)
            self.executor.run_remote(host, f"rm -f {temp_file}")
            print(f"[{host}] Wrote MDS perf dump {dest_file} → {results_dir}/")

        if not found:
            print(
                f"Warning: no MDS admin sockets found for perf dump "
                f"(load point {loadpoint})"
            )

    def rebuild_filesystem(self, settings, ganesha_manager=None, results_dir=None):
        self._run_ceph(self.admin, "config set mon mon_allow_pool_delete true")
        self._run_ceph(self.admin, "config set global mon_max_pg_per_osd 1000")
        if self.config.ganesha_enabled and ganesha_manager:
            ganesha_manager.cleanup_ganesha()
        for fs in self.get_fs_names():
            self._remove_mds_service(fs)
            # Give MDS processes time to drop mon sessions before fail/rm.
            # Otherwise: fs rm fails, pools get deleted under a leftover FS,
            # fs new fails ("already exists"), and MDS stay forever in standby
            # against a non-joinable / pool-less filesystem.
            time.sleep(2)
            self._run_ceph(
                self.admin, f"fs fail {fs} --yes-i-really-mean-it || true"
            )
            self._run_ceph(
                self.admin, f"fs rm {fs} --yes-i-really-mean-it || true"
            )
            # Confirm the FS is gone before deleting pools (avoids orphan FS map).
            ls_raw = "[]"
            for _ in range(30):
                ls_raw = self._run_ceph(
                    self.admin, "fs ls --format json || echo []"
                )
                fs_list = self.safe_json_load(ls_raw, [])
                names = []
                if isinstance(fs_list, list):
                    for entry in fs_list:
                        if isinstance(entry, dict) and entry.get("name"):
                            names.append(entry["name"])
                if fs not in names:
                    break
                # Still present — retry rm
                self._run_ceph(
                    self.admin, f"fs rm {fs} --yes-i-really-mean-it || true"
                )
                time.sleep(1)
            else:
                raise RuntimeError(
                    f"Failed to remove filesystem '{fs}' before recreate; "
                    f"refusing to delete pools (would leave a pool-less FS). "
                    f"fs ls={ls_raw!r}"
                )

            self._run_ceph(
                self.admin,
                f"osd pool delete {fs}_metadata {fs}_metadata "
                f"--yes-i-really-really-mean-it || true",
            )
            self._run_ceph(
                self.admin,
                f"osd pool delete {fs}_data {fs}_data "
                f"--yes-i-really-really-mean-it || true",
            )

            osd_hosts_count = 0
            try:
                osd_tree_raw = self._run_ceph(
                    self.admin, "osd tree --format json"
                )
                osd_tree = self.safe_json_load(osd_tree_raw, {})
                nodes = osd_tree.get("nodes", [])
                osd_hosts_count = sum(1 for node in nodes if node.get("type") == "host")
            except Exception:
                pass

            self._run_ceph(
                self.admin, f"osd pool create {fs}_metadata", check=True
            )
            self._run_ceph(
                self.admin, f"osd pool create {fs}_data", check=True
            )

            if 0 < osd_hosts_count < 3:
                for pool in [f"{fs}_metadata", f"{fs}_data"]:
                    self._run_ceph(self.admin, f"osd pool set {pool} size 2")
                    self._run_ceph(self.admin, f"osd pool set {pool} min_size 1")
            self._run_ceph(
                self.admin,
                f"fs new {fs} {fs}_metadata {fs}_data",
                check=True,
            )
            # Ensure ranks are eligible (fs fail leaves joinable=false; new FS
            # should already be joinable, but force it for recreate safety).
            self._run_ceph(self.admin, f"fs set {fs} joinable true || true")
            self._deploy_mds(fs, settings)
            self._wait_for_mds_active(fs)
            self.setup_client_auth(fs)
        self.distribute_keys_and_config()

    def _remove_mds_service(self, fs):
        """Tear down MDS daemons for the given filesystem. Subclasses implement this."""
        raise NotImplementedError

    def _deploy_mds(self, fs, settings):
        """Deploy MDS daemons for the given filesystem. Subclasses implement this."""
        raise NotImplementedError

    def _select_mds_hosts(self, fs, count):
        """Select host placement for MDS daemons (active + standbys)."""
        num_mdss = len(self.mdss)
        if num_mdss == 0:
            raise RuntimeError("No MDS hosts available in inventory")
        num_hosts = min(count + 2, num_mdss)
        start_idx = (
            self.get_fs_names().index(fs) if fs in self.get_fs_names() else 0
        ) % num_mdss
        return [self.mdss[(start_idx + i) % num_mdss] for i in range(num_hosts)]

    def _wait_for_mds_active(self, fs, timeout_iters=60, sleep_secs=5):
        last_status = ""
        for i in range(timeout_iters):
            status_raw = self._run_ceph(
                self.admin, f"fs status {fs} --format json"
            )
            last_status = status_raw
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
                    return
            elif isinstance(mdsmap, list):
                if any(
                    str(e.get("state", "")).lower() in ["active", "up:active"]
                    or "active" in str(e.get("state", "")).lower()
                    for e in mdsmap
                ):
                    return
            if i % 6 == 0:
                print(
                    f"[wait] {fs}: no active MDS yet "
                    f"(iter {i + 1}/{timeout_iters}); status={status_raw[:500]}"
                )
            time.sleep(sleep_secs)

        # Dump extra diagnostics so a hang is actionable instead of silent.
        extras = []
        for args in (
            "fs ls",
            f"fs get {fs} || true",
            "fs dump || true",
            "osd lspools || true",
        ):
            try:
                extras.append(
                    f"$ ceph {args}\n{self._run_ceph(self.admin, args)}"
                )
            except Exception as e:
                extras.append(f"$ ceph {args}\n<error: {e}>")
        raise RuntimeError(
            f"Timed out waiting for an active MDS on filesystem '{fs}'.\n"
            f"Last fs status:\n{last_status}\n" + "\n".join(extras)
        )

    def get_fs_names(self):
        return self.fs_names

    # `ceph fs set` only accepts filesystem-map fields. MDS daemon options
    # (mds_cache_memory_limit, etc.) go through `ceph config set mds`.
    _FS_SET_VARS = frozenset(
        {
            "max_mds",
            "allow_dirfrags",
            "allow_new_snaps",
            "allow_standby_replay",
            "bal_rank_mask",
            "balance_automate",
            "balancer",
            "cluster_down",
            "down",
            "inline_data",
            "joinable",
            "max_file_size",
            "max_xattr_size",
            "min_compat_client",
            "refuse_client_session",
            "refuse_standby_for_another_fs",
            "session_autoclose",
            "session_timeout",
            "standby_count_wanted",
        }
    )

    def apply_fs_settings(self, settings):
        """Apply mds_settings to the cluster.

        Daemon options (``mds_*``) use ``ceph config set mds``. Filesystem-map
        fields (``max_mds``, ...) use ``ceph fs set``. ``cpus`` is a host
        placement knob handled at deploy time, not a Ceph option.

        Raises if any set command fails, or if a subsequent MDS
        ``config diff`` does not show the expected live values.
        """
        fs_set_pending = {}
        for k, v in settings.items():
            if k == "cpus":
                continue
            val = CommonUtils.format_si_units(v)
            if k in self._FS_SET_VARS:
                fs_set_pending[k] = val
                continue
            # Settings keys are already mds_* (e.g. mds_cache_memory_limit);
            # do not double-prefix with mds_.
            key = k if k.startswith("mds_") else f"mds_{k}"
            self._run_ceph(
                self.admin, f"config set mds {key} {val}", check=True
            )
        # Apply fs-map options last (max_mds can change rank count).
        for k, val in fs_set_pending.items():
            for fs in self.get_fs_names():
                self._run_ceph(
                    self.admin, f"fs set {fs} {k} {val}", check=True
                )
        self._verify_mds_settings(settings, fs_set_pending)

    def _mds_config_from_settings(self, settings):
        """Return ``{option: formatted_value}`` for daemon options we applied."""
        expected = {}
        for k, v in settings.items():
            if k == "cpus" or k in self._FS_SET_VARS:
                continue
            key = k if k.startswith("mds_") else f"mds_{k}"
            expected[key] = CommonUtils.format_si_units(v)
        return expected

    def _extract_json_object(self, raw):
        """Parse a JSON object from ceph CLI output, skipping leading noise."""
        if not raw:
            return {}
        data = self.safe_json_load(raw, {})
        if isinstance(data, dict) and data:
            return data
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            data = self.safe_json_load(raw[start : end + 1], {})
            if isinstance(data, dict):
                return data
        return {}

    def _parse_config_diff(self, raw):
        """Map option name → effective value from an MDS ``config diff``.

        Admin-socket output is::

            {"config_diff": {"diff": {"opt": {"default": ..., "mon": ..., "final": ...}}}}
        """
        data = self._extract_json_object(raw)
        diff = data.get("config_diff", data)
        if isinstance(diff, dict):
            diff = diff.get("diff", diff)
        if not isinstance(diff, dict):
            return {}
        out = {}
        for key, entry in diff.items():
            if key in ("current", "defaults") and isinstance(entry, dict):
                for inner_k, inner_v in entry.items():
                    out[inner_k] = inner_v
                continue
            if isinstance(entry, dict):
                if "final" in entry:
                    out[key] = entry["final"]
                elif "override" in entry:
                    out[key] = entry["override"]
                elif "mon" in entry:
                    out[key] = entry["mon"]
                else:
                    out[key] = entry
            else:
                out[key] = entry
        return out

    def _parse_config_get(self, raw, key):
        data = self._extract_json_object(raw)
        if key in data:
            return data[key]
        inner = data.get("config_get", {})
        if isinstance(inner, dict) and key in inner:
            return inner[key]
        return None

    @staticmethod
    def _normalize_config_value(value):
        """Turn a Ceph config value into a comparable number, bool, or string."""
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value) if value == int(value) else value
        s = str(value).strip().replace(" ", "")
        if s.lower() in ("true", "yes", "on"):
            return True
        if s.lower() in ("false", "no", "off"):
            return False
        m = re.fullmatch(r"(-?\d+(?:\.\d+)?)([KMGTPEkmgtpe]i?B?)?", s)
        if not m:
            return s.lower()
        num = float(m.group(1))
        unit = m.group(2) or ""
        if unit.lower().endswith("b"):
            unit = unit[:-1]
        iec = {
            "ki": 1024,
            "mi": 1024**2,
            "gi": 1024**3,
            "ti": 1024**4,
            "pi": 1024**5,
            "ei": 1024**6,
        }
        si = {
            "k": 1000,
            "m": 1000**2,
            "g": 1000**3,
            "t": 1000**4,
            "p": 1000**5,
            "e": 1000**6,
        }
        unit_l = unit.lower()
        if unit_l in iec:
            return int(num * iec[unit_l])
        if unit_l in si:
            return int(num * si[unit_l])
        if num == int(num):
            return int(num)
        return num

    def _config_values_match(self, expected, actual):
        a = self._normalize_config_value(expected)
        b = self._normalize_config_value(actual)
        if a == b:
            return True
        try:
            return float(a) == float(b)
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _asok_error_is_unusable(exc):
        text = str(exc).lower()
        return any(
            s in text
            for s in (
                "af_unix path too long",
                "connection refused",
                "no such file",
                "socket operation on non-socket",
            )
        )

    def _live_mds_asoks(self):
        """Return ``[(host, mds_id, asok), ...]`` for sockets that exist."""
        live = []
        seen = set()
        for host, mds_id, asok in self._iter_mds_admin_sockets():
            key = (host, asok)
            if key in seen:
                continue
            seen.add(key)
            if self._asok_exists(host, asok):
                live.append((host, mds_id, asok))
        return live

    def _collect_mds_config_diff(self, host, mds_id, asok):
        print(f"[{host}] Running MDS 'config diff' for mds.{mds_id} via {asok}...")
        raw = self.executor.run_remote(
            host,
            self._admin_daemon_cmd(asok, "config diff"),
            check=True,
        )
        parsed = self._extract_json_object(raw)
        if parsed:
            pretty = json.dumps(parsed, indent=2, default=str)
        else:
            pretty = raw
        print(f"[{host}] mds.{mds_id} config diff:\n{pretty}")
        return raw

    def _live_mds_config_value(self, host, asok, key, diff_vals):
        if key in diff_vals:
            return diff_vals[key]
        raw = self.executor.run_remote(
            host,
            self._admin_daemon_cmd(asok, f"config get {key}"),
            check=True,
        )
        return self._parse_config_get(raw, key)

    def _verify_mds_settings(self, settings, fs_set_pending, retries=8, sleep_secs=2):
        """Collect MDS ``config diff`` and require live values to match *settings*."""
        expected = self._mds_config_from_settings(settings)
        for attempt in range(retries):
            try:
                sockets = self._live_mds_asoks()
                if expected and not sockets:
                    raise RuntimeError(
                        "No MDS admin sockets found; cannot collect "
                        "config diff after apply_fs_settings"
                    )
                usable = 0
                for host, mds_id, asok in sockets:
                    try:
                        raw = self._collect_mds_config_diff(host, mds_id, asok)
                    except Exception as e:
                        if self._asok_error_is_unusable(e):
                            print(
                                f"[{host}] Skipping unusable MDS socket "
                                f"{asok}: {e}"
                            )
                            continue
                        raise
                    usable += 1
                    if not expected:
                        continue
                    live = self._parse_config_diff(raw)
                    mismatches = []
                    for key, exp in expected.items():
                        got = self._live_mds_config_value(host, asok, key, live)
                        if got is None:
                            mismatches.append(
                                f"{key}: expected {exp!r}, not present in "
                                f"config diff / config get"
                            )
                        elif not self._config_values_match(exp, got):
                            mismatches.append(
                                f"{key}: expected {exp!r}, live {got!r}"
                            )
                        else:
                            print(
                                f"[{host}] mds.{mds_id} {key}="
                                f"{got!r} matches expected {exp!r}"
                            )
                    if mismatches:
                        raise RuntimeError(
                            f"MDS config mismatch on mds.{mds_id}@{host}: "
                            + "; ".join(mismatches)
                        )
                if expected and usable == 0:
                    raise RuntimeError(
                        "No usable MDS admin sockets found; cannot collect "
                        "config diff after apply_fs_settings"
                    )
                for k, val in fs_set_pending.items():
                    for fs in self.get_fs_names():
                        raw = self._run_ceph(
                            self.admin, f"fs get {fs} --format json", check=True
                        )
                        fs_data = self._extract_json_object(raw)
                        mdsmap = fs_data.get("mdsmap", fs_data)
                        got = mdsmap.get(k) if isinstance(mdsmap, dict) else None
                        if got is None or not self._config_values_match(val, got):
                            raise RuntimeError(
                                f"fs {fs} {k}: expected {val!r}, live {got!r}"
                            )
                        print(f"[fs {fs}] {k}={got!r} matches expected {val!r}")
                return
            except Exception as e:
                if attempt + 1 < retries:
                    print(
                        f"[wait] MDS settings not live yet "
                        f"(attempt {attempt + 1}/{retries}): {e}"
                    )
                    time.sleep(sleep_secs)
                    continue
                raise

    def setup_client_auth(self, fs):
        self._run_ceph(self.admin, f"fs authorize {fs} client.0 / rwps")
        self._run_ceph(
            self.admin,
            "auth get client.0 -o /etc/ceph/ceph.client.0.keyring",
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

    def _copy_log_to_results(self, server_name, src_log, dest_log, results_dir):
        """Copy a remote log file to the admin host results directory."""
        self.executor.run_remote(server_name, f"sudo cp {src_log} /tmp/{dest_log}")
        user, _, _ = self.executor.get_ssh_details(server_name)
        self.executor.run_remote(
            server_name, f"sudo chown {user}:{user} /tmp/{dest_log}"
        )
        admin_user, admin_host, admin_port = self.executor.get_ssh_details(self.admin)
        copy_cmd = (
            f"scp -o StrictHostKeyChecking=no -P {admin_port} "
            f"/tmp/{dest_log} {admin_user}@{admin_host}:{results_dir}/"
        )
        self.executor.run_remote(server_name, copy_cmd)
        self.executor.run_remote(server_name, f"rm -f /tmp/{dest_log}")
        self.executor.run_remote(server_name, f"sudo truncate -s 0 {src_log}")

    def _collect_mds_logs(self, loadpoint, results_dir):
        """Collect MDS logs. Subclasses override for their deployment model."""
        pass

    def is_mds_lockstat_enabled(self):
        # Prefer mds.lockstat; fall back to legacy specstorage.lockstat
        mds_ls = (self.config.get("mds", {}) or {}).get("lockstat", {}) or {}
        if "enabled" in mds_ls:
            return bool(mds_ls.get("enabled"))
        return bool(
            self.config.get("specstorage", {}).get("lockstat", {}).get("enabled")
        )

    def is_mds_perf_record_enabled(self):
        return bool(self.config.get("mds", {}).get("perf_record", False))

    def is_mds_logging_enabled(self):
        return bool(self._mds_logging_cfg().get("enabled"))
