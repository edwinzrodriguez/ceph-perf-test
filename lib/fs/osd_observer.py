"""OSD lockstat, admin-socket perf dump, and periodic dump collection.

Mirrors the MDS hooks used during a loadpoint window. Lockstat uses the same
``ceph-lockstat`` invocation shape as MDS (``python3 <tool> osd.<id> ...``).
A missing binary or admin socket is reported explicitly when the feature is
enabled.
"""

import json
import os
import shlex
import subprocess

from cephfs_perf_lib import CommonUtils
from lib.config_loader import role_observability
from lib.run_logging import get_logger

log = get_logger(__name__)


class OsdObserver:
    def __init__(self, executor, config):
        self.executor = executor
        self.config = config
        self.admin = config.admin_host
        self.lockstat_exists = {}
        self._periodic_jobs = []
        self._periodic_dest = None
        self._discovered_hosts = None

    def _obs(self):
        return role_observability(self.config._config, "osd")

    def _lockstat_cfg(self):
        return self._obs().get("lockstat") or {}

    def _perf_record_cfg(self):
        return self._obs().get("perf_record") or {}

    def _perf_dump_cfg(self):
        return self._obs().get("perf_dump") or {}

    def is_lockstat_enabled(self):
        return bool(self._lockstat_cfg().get("enabled"))

    def is_perf_record_enabled(self):
        return bool(self._perf_record_cfg().get("enabled"))

    def is_perf_dump_enabled(self):
        return bool(self._perf_dump_cfg().get("enabled"))

    def _ceph_env(self):
        return self.config.env_vars

    def _ceph_bin(self):
        return self.config.resolve_ceph_binary()

    def _run_dir(self):
        return (self.config.get("mds") or {}).get("run_dir", "/var/run/ceph")

    def hosts(self):
        """Inventory ``osds`` hosts, or hosts discovered from ``ceph osd tree``."""
        configured = list(self.config.osds or [])
        if configured:
            return configured
        if self._discovered_hosts is not None:
            return list(self._discovered_hosts)
        hosts = []
        tree = {}
        try:
            raw = self.executor.run_remote(
                self.admin,
                CommonUtils.with_env_exports(
                    f"{self._ceph_bin()} osd tree --format json",
                    self._ceph_env(),
                    sudo=True,
                ),
            )
            tree = json.loads(raw) if raw and raw.strip().startswith("{") else {}
        except Exception as exc:
            log.warning("Could not discover OSD hosts from ceph osd tree: %s", exc)
            tree = {}
        names = {
            node.get("name")
            for node in (tree or {}).get("nodes", [])
            if node.get("type") == "host" and node.get("name")
        }
        # Prefer inventory names that match; otherwise use the tree hostnames.
        inventory_names = set(self.config.all_hosts_meta.keys())
        for name in sorted(names):
            if not inventory_names or name in inventory_names:
                hosts.append(name)
        if not hosts:
            hosts = sorted(names)
        self._discovered_hosts = hosts
        return list(hosts)

    def _asok_exists(self, host, asok_path):
        out = self.executor.run_remote(
            host, f"test -S {asok_path} && echo OK || echo MISSING"
        ).strip()
        return "OK" in out

    def iter_osds(self):
        """Yield ``(host, osd_id, asok_path)`` for each ceph-osd admin socket."""
        run_dir = self._run_dir()
        seen = set()
        for host in self.hosts():
            listing = self.executor.run_remote(
                host,
                f"find {shlex.quote(run_dir)} /var/run/ceph -type s "
                f"-name 'ceph-osd.*.asok' 2>/dev/null || true",
            ).strip()
            for asok in listing.split():
                key = (host, asok)
                if not asok or key in seen:
                    continue
                base = asok.rsplit("/", 1)[-1]
                if base.startswith("ceph-osd.") and base.endswith(".asok"):
                    osd_id = base[len("ceph-osd.") : -len(".asok")]
                else:
                    osd_id = base
                seen.add(key)
                yield host, osd_id, asok

    def _lockstat_path(self):
        cfg = self._lockstat_cfg()
        return self.config.expand_env(
            cfg.get("path", "/usr/local/bin/ceph-lockstat")
        )

    def _check_lockstat_exists(self, host, path):
        if host not in self.lockstat_exists:
            check = self.executor.run_remote(
                host,
                f"test -f {shlex.quote(path)} && echo EXISTS || echo MISSING",
            ).strip()
            self.lockstat_exists[host] = "EXISTS" in check
        return self.lockstat_exists[host]

    def _require_lockstat_tool(self, host, path):
        if self._check_lockstat_exists(host, path):
            return True
        raise RuntimeError(
            f"[{host}] ceph-lockstat not found at {path}; "
            "observability.osd.lockstat is enabled"
        )

    def _lockstat_cmd(self, osd_id, action, threshold=None):
        path = self._lockstat_path()
        cmd = f"python3 {path} osd.{osd_id} {action}"
        if threshold is not None:
            cmd += f" --threshold {threshold}"
        return CommonUtils.with_env_exports(cmd, self._ceph_env(), sudo=True)

    def _each_osd_or_fail(self):
        found = list(self.iter_osds())
        if not found:
            raise RuntimeError(
                "observability.osd is enabled but no ceph-osd admin sockets "
                f"were found on hosts {self.hosts() or '(none)'}"
            )
        return found

    def start_lockstat(self):
        if not self.is_lockstat_enabled():
            return
        path = self._lockstat_path()
        threshold = self._lockstat_cfg().get("threshold", 0)
        daemons = self._each_osd_or_fail()
        for host, osd_id, _asok in daemons:
            self._require_lockstat_tool(host, path)
            log.info(
                "[%s] Starting lockstat for osd.%s via %s with threshold %s",
                host,
                osd_id,
                path,
                threshold,
            )
            self.executor.run_remote(
                host, self._lockstat_cmd(osd_id, "start", threshold=threshold)
            )

    def stop_lockstat(self):
        if not self.is_lockstat_enabled():
            return
        path = self._lockstat_path()
        for host, osd_id, _asok in self.iter_osds():
            if not self.lockstat_exists.get(host):
                continue
            log.info("[%s] Stopping lockstat for osd.%s via %s", host, osd_id, path)
            self.executor.run_remote(host, self._lockstat_cmd(osd_id, "stop"))

    def reset_lockstat(self):
        if not self.is_lockstat_enabled():
            return
        path = self._lockstat_path()
        daemons = self._each_osd_or_fail()
        for host, osd_id, _asok in daemons:
            self._require_lockstat_tool(host, path)
            log.info("[%s] Resetting lockstat for osd.%s", host, osd_id)
            self.executor.run_remote(host, self._lockstat_cmd(osd_id, "reset"))

    def dump_lockstat(self, loadpoint, results_dir=None, settings=None, lp_cfg=None):
        if not self.is_lockstat_enabled():
            return
        path = self._lockstat_path()
        for host, osd_id, _asok in self.iter_osds():
            if not self._check_lockstat_exists(host, path):
                log.error(
                    "[%s] ceph-lockstat not found at %s; skip osd.%s dump",
                    host,
                    path,
                    osd_id,
                )
                continue
            log.info(
                "[%s] Dumping lockstat for osd.%s (Load Point %s)",
                host,
                osd_id,
                loadpoint,
            )
            if not results_dir:
                continue
            dump_cmd = CommonUtils.with_env_exports(
                f"python3 {path} osd.{osd_id} dump --detail",
                self._ceph_env(),
            )
            CommonUtils.dump_lockstat_common(
                self.executor,
                host,
                loadpoint,
                results_dir,
                f"osd.{osd_id}",
                dump_cmd,
                self.admin,
                settings=settings,
                lp_cfg=lp_cfg,
            )

    def _admin_daemon_cmd(self, asok, args):
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

    def reset_perf_counters(self):
        found = False
        for host, osd_id, asok in self.iter_osds():
            if not self._asok_exists(host, asok):
                log.warning(
                    "[%s] OSD admin socket not found for osd.%s (%s); skip perf reset",
                    host,
                    osd_id,
                    asok,
                )
                continue
            found = True
            log.info(
                "[%s] Resetting OSD perf counters for osd.%s via %s",
                host,
                osd_id,
                asok,
            )
            self.executor.run_remote(host, self._admin_daemon_cmd(asok, "perf reset all"))
        if not found:
            log.warning("No OSD admin sockets found for perf counter reset")

    def dump_perf_counters(self, loadpoint, results_dir=None, settings=None, lp_cfg=None):
        if not results_dir:
            log.info("Skipping OSD perf dump for load point %s: no results_dir", loadpoint)
            return
        found = False
        for host, osd_id, asok in self.iter_osds():
            if not self._asok_exists(host, asok):
                log.warning(
                    "[%s] OSD admin socket not found for osd.%s (%s); skip perf dump",
                    host,
                    osd_id,
                    asok,
                )
                continue
            found = True
            log.info(
                "[%s] Dumping OSD perf counters for osd.%s (Load Point %s) via %s",
                host,
                osd_id,
                loadpoint,
                asok,
            )
            dest = self._dest_name(osd_id, host, loadpoint, settings, lp_cfg)
            self._copy_admin_json(host, asok, results_dir, "perf dump", dest)
            log.info("[%s] Wrote OSD perf dump %s -> %s/", host, dest, results_dir)
        if not found:
            log.warning(
                "No OSD admin sockets found for perf dump (load point %s)", loadpoint
            )

    def _dest_name(self, osd_id, host, loadpoint, settings, lp_cfg):
        target = f"osd.{osd_id}"
        if settings is not None:
            return (
                f"{CommonUtils.get_workload_base_name(target, 'perf_dump', host, loadpoint, settings, lp_cfg)}.json"
            )
        return f"{target}_perf_dump_{host}_lp{int(loadpoint):02d}.json"

    def _copy_admin_json(self, host, asok, results_dir, admin_subcommand, dest_filename):
        temp_file = f"/tmp/{dest_filename}"
        dump_cmd = self._admin_daemon_cmd(asok, admin_subcommand)
        self.executor.run_remote(host, f"{dump_cmd} | sudo tee {temp_file} > /dev/null")
        user, _, _ = self.executor.get_ssh_details(host)
        self.executor.run_remote(host, f"sudo chown {user}:{user} {temp_file}")
        admin_user, admin_host, admin_port = self.executor.get_ssh_details(self.admin)
        copy_cmd = (
            f"scp -o StrictHostKeyChecking=no -P {admin_port} "
            f"{temp_file} {admin_user}@{admin_host}:{results_dir}/"
        )
        self.executor.run_remote(host, copy_cmd)
        self.executor.run_remote(host, f"rm -f {temp_file}")

    def _script_remote(self):
        return self._perf_dump_cfg().get(
            "script", "/cephfs_perf/mds_perf_dump_collector.py"
        )

    def _script_local(self):
        here = os.path.dirname(os.path.abspath(__file__))
        return os.path.abspath(os.path.join(here, "..", "..", "mds_perf_dump_collector.py"))

    def _ensure_script(self, host):
        remote = self._script_remote()
        local = self._script_local()
        if not os.path.isfile(local):
            return remote
        remote_dir = os.path.dirname(remote) or "/cephfs_perf"
        user, _, _ = self.executor.get_ssh_details(host)
        self.executor.run_remote(
            host, f"sudo mkdir -p {remote_dir} && sudo chown {user}:{user} {remote_dir}"
        )
        u, h, p = self.executor.get_ssh_details(host)
        subprocess.run(
            [
                "scp",
                "-o",
                "StrictHostKeyChecking=no",
                "-P",
                str(p),
                local,
                f"{u}@{h}:{remote}",
            ],
            check=False,
        )
        self.executor.run_remote(host, f"chmod +x {remote} || true")
        return remote

    def start_periodic_perf_dump(self, loadpoint, results_dir=None):
        cfg = self._perf_dump_cfg()
        if not self.is_perf_dump_enabled():
            return
        if not results_dir:
            log.info(
                "Skipping periodic OSD perf dump for load point %s: no results_dir",
                loadpoint,
            )
            return
        self.stop_periodic_perf_dump()
        interval = max(1, int(cfg.get("interval", 60)))
        histogram = cfg.get("histogram")
        no_histogram = not histogram
        dest = f"{results_dir}/lp{int(loadpoint):02d}"
        self.executor.run_remote(self.admin, f"mkdir -p {dest}")
        jobs = []
        for host, osd_id, asok in self.iter_osds():
            if not self._asok_exists(host, asok):
                log.warning(
                    "[%s] OSD admin socket not found for osd.%s (%s); skip periodic perf dump",
                    host,
                    osd_id,
                    asok,
                )
                continue
            script = self._ensure_script(host)
            jobs.append(
                self._start_collector(
                    host, osd_id, asok, loadpoint, script, interval, histogram, no_histogram
                )
            )
        self._periodic_jobs = jobs
        self._periodic_dest = dest
        if jobs:
            log.info(
                "Started %s local OSD perf dump collector(s) for load point %s",
                len(jobs),
                loadpoint,
            )
        else:
            log.warning(
                "No OSD admin sockets found for periodic perf dump (load point %s)",
                loadpoint,
            )

    def _start_collector(
        self, host, osd_id, asok, loadpoint, script, interval, histogram, no_histogram
    ):
        lp_tag = f"{int(loadpoint):02d}"
        safe_id = str(osd_id).replace("/", "_")
        local_dir = f"/tmp/osd_perf_dump_lp{lp_tag}_{safe_id}"
        pid_file = f"{local_dir}.pid"
        log_file = f"{local_dir}.log"
        env = CommonUtils.expand_env_vars_map(self._ceph_env() or {})
        env_args = " ".join(
            f"--env {shlex.quote(f'{k}={v}')}" for k, v in env.items()
        )
        hist_arg = "--no-histogram"
        if histogram and not no_histogram:
            hist_arg = f"--histogram-cmd {shlex.quote(str(histogram))}"
        self.executor.run_remote(
            host,
            f"if [ -f {shlex.quote(pid_file)} ]; then "
            f"kill $(cat {shlex.quote(pid_file)}) 2>/dev/null || true; fi; "
            f"rm -rf {shlex.quote(local_dir)} {shlex.quote(pid_file)}; "
            f"mkdir -p {shlex.quote(local_dir)}",
        )
        collector_cmd = (
            f"python3 {shlex.quote(script)} "
            f"--host {shlex.quote(host)} "
            f"--asok {shlex.quote(asok)} "
            f"--ceph-bin {shlex.quote(self._ceph_bin())} "
            f"--output-dir {shlex.quote(local_dir)} "
            f"--interval {interval} "
            f"--pid-file {shlex.quote(pid_file)} "
            f"{hist_arg} "
            f"{env_args}"
        )
        start_cmd = (
            f"nohup {collector_cmd} >{shlex.quote(log_file)} 2>&1 </dev/null & "
            f"echo $! > {shlex.quote(pid_file)}"
        )
        log.info(
            "[%s] Starting local OSD perf dump collector for osd.%s (interval=%ss) -> %s/",
            host,
            osd_id,
            interval,
            local_dir,
        )
        self.executor.run_remote(host, start_cmd)
        return {
            "host": host,
            "osd_id": osd_id,
            "local_dir": local_dir,
            "pid_file": pid_file,
            "log_file": log_file,
        }

    def stop_periodic_perf_dump(self):
        jobs = list(self._periodic_jobs or [])
        dest = self._periodic_dest
        self._periodic_jobs = []
        self._periodic_dest = None
        if not jobs:
            return
        if dest:
            self.executor.run_remote(self.admin, f"mkdir -p {dest}")
        for job in jobs:
            self._stop_collector(job, dest)

    def _stop_collector(self, job, dest_dir):
        host = job["host"]
        local_dir = job["local_dir"]
        pid_file = job["pid_file"]
        log.info(
            "[%s] Stopping local OSD perf dump collector for osd.%s",
            host,
            job.get("osd_id"),
        )
        self.executor.run_remote(
            host,
            f"if [ -f {shlex.quote(pid_file)} ]; then "
            f"kill $(cat {shlex.quote(pid_file)}) 2>/dev/null || true; "
            f"for i in 1 2 3 4 5 6 7 8 9 10; do "
            f"  kill -0 $(cat {shlex.quote(pid_file)} 2>/dev/null) 2>/dev/null || break; "
            f"  sleep 0.5; "
            f"done; "
            f"kill -9 $(cat {shlex.quote(pid_file)} 2>/dev/null) 2>/dev/null || true; "
            f"fi",
        )
        if not dest_dir:
            self.executor.run_remote(
                host,
                f"rm -rf {shlex.quote(local_dir)} {shlex.quote(pid_file)} "
                f"{shlex.quote(job.get('log_file', ''))}",
            )
            return
        admin_user, admin_host, admin_port = self.executor.get_ssh_details(self.admin)
        listing = self.executor.run_remote(
            host, f"ls {shlex.quote(local_dir)}/*.json 2>/dev/null || true"
        ).strip()
        files = [item for item in listing.split() if item]
        if files:
            log.info(
                "[%s] Copying %s periodic OSD perf dump file(s) -> %s/",
                host,
                len(files),
                dest_dir,
            )
            copy_cmd = (
                f"scp -o StrictHostKeyChecking=no -P {admin_port} "
                f"{shlex.quote(local_dir)}/*.json "
                f"{admin_user}@{admin_host}:{shlex.quote(dest_dir)}/"
            )
            self.executor.run_remote(host, copy_cmd)
        else:
            log.info("[%s] No periodic OSD perf dump files found in %s", host, local_dir)
        self.executor.run_remote(
            host,
            f"rm -rf {shlex.quote(local_dir)} {shlex.quote(pid_file)} "
            f"{shlex.quote(job.get('log_file', ''))}",
        )
