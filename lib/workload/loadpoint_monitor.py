"""Shared RUN-start and loadpoint-end observability hooks.

Workload runners keep workload-specific extras (phase lockstat, result
injection). Markers stay ``Starting tests...``, ``Starting RUN phase``, and
the per-workload ``Finished ...`` / ``Tests finished`` lines.
"""

import json
import os
import subprocess
import threading

from cephfs_perf_lib import CommonUtils
from lib.config_loader import (
    client_lockstat,
    client_perf_record,
    role_observability,
    section_perf_record,
)
from lib.fs.osd_observer import OsdObserver
from lib.run_logging import get_logger

log = get_logger(__name__)

_CLIENT_EXECUTABLE = {
    "fio": "fio",
    "rbd": "fio",
    "cephfs_tool": "cephfs-tool",
    "mdsbench": "cephfs-mdsbench",
    "rados_bench": "rados",
}


class LoadpointMonitor:
    def __init__(
        self,
        runner,
        *,
        workload_name,
        config_section=None,
        cephfs_manager=None,
        ganesha_manager=None,
        results_dir=None,
        settings=None,
        payload=None,
        loadpoints=None,
        total_loadpoints=None,
        initial_lp=0,
        finish_markers=(),
        mds_perf_trigger="immediate",
        run_phase_fallback=None,
        after_loadpoint_end=None,
        osd_observer=None,
    ):
        self.runner = runner
        self.config = runner.config
        self.executor = runner.executor
        self.workload_name = workload_name
        self.config_section = config_section or workload_name
        self.cephfs_manager = cephfs_manager
        self.ganesha_manager = ganesha_manager
        self.results_dir = results_dir
        self.settings = settings
        self.payload = payload if payload is not None else settings
        self.perf_settings = self.payload
        self.loadpoints = list(loadpoints or [])
        self.total_loadpoints = (
            total_loadpoints if total_loadpoints is not None else len(self.loadpoints)
        )
        self.current_lp = initial_lp
        self.finish_markers = tuple(finish_markers or ())
        self.mds_perf_trigger = mds_perf_trigger
        self.run_phase_fallback = run_phase_fallback
        self.after_loadpoint_end = after_loadpoint_end
        self.osd_observer = osd_observer or getattr(runner, "osd_observer", None)
        if self.osd_observer is None:
            self.osd_observer = OsdObserver(self.executor, self.config)
            runner.osd_observer = self.osd_observer

        self.run_phase_started = False
        self._mds_perf_started = False
        self._other_perf_started = False
        self.logging_triggered = False
        self.client_lockstat_started = False
        self._ended_for = None
        self.perf_threads = []
        self.ganesha_perf_dumps = {}
        self.ganesha_lockstat = {}
        self.client_lockstat = client_lockstat(self.config._config, self.config_section)
        self._client_perf = self._resolve_client_perf()

    def _resolve_client_perf(self):
        executable = _CLIENT_EXECUTABLE.get(self.config_section)
        if not executable:
            return None
        return client_perf_record(
            self.config._config, self.config_section, default_executable=executable
        )

    def current_lp_cfg(self):
        index = self.current_lp - 1
        if index < 0 or index >= len(self.loadpoints):
            return None
        item = self.loadpoints[index]
        return item if isinstance(item, dict) else None

    def handle_line(self, line):
        if "Starting tests..." in line:
            self._on_starting_tests()
        if "Starting RUN phase" in line:
            self.on_run_phase_start()
        elif (
            self.run_phase_fallback
            and not self.run_phase_started
            and self.run_phase_fallback(line)
        ):
            log.info(
                "Detected RUN phase start from run progress "
                "(Prime did not log 'Starting RUN phase') for Load Point %s",
                self.current_lp,
            )
            self.on_run_phase_start()
        self._maybe_start_perf(line)
        if any(marker in line for marker in self.finish_markers):
            self.on_loadpoint_end()

    def finish(self):
        if self._has_mds():
            self.cephfs_manager.stop_periodic_perf_dump()
        if self.osd_observer:
            self.osd_observer.stop_periodic_perf_dump()
        for thread in self.perf_threads:
            thread.join()

    def _on_starting_tests(self):
        self.current_lp += 1
        self.run_phase_started = False
        self._mds_perf_started = False
        self._other_perf_started = False
        self.logging_triggered = False
        self.client_lockstat_started = False
        self._ended_for = None
        total = self.total_loadpoints
        pct = int(100 * self.current_lp / total) if total else 0
        log.info(
            "Detected Starting tests... Load Point: %s/%s (%s%% done)",
            self.current_lp,
            total,
            pct,
        )

    def on_run_phase_start(self, fallback=False):
        del fallback
        if self.run_phase_started:
            return
        self.run_phase_started = True
        manager = self.cephfs_manager if self._has_mds() else None
        if manager and manager.is_mds_lockstat_enabled():
            log.info("Resetting MDS lockstat for Load Point %s", self.current_lp)
            manager.reset_lockstat()
        if manager:
            log.info("Resetting MDS perf counters for Load Point %s", self.current_lp)
            manager.reset_perf_counters()
            manager.start_periodic_perf_dump(self.current_lp, self.results_dir)
        if (
            manager
            and manager.is_mds_logging_enabled()
            and not self.logging_triggered
        ):
            log.info("Triggering MDS logging for Load Point %s", self.current_lp)
            manager.start_fs_logging(self.current_lp)
            self.logging_triggered = True

        osd = self.osd_observer
        if osd and osd.is_lockstat_enabled():
            log.info("Resetting OSD lockstat for Load Point %s", self.current_lp)
            osd.reset_lockstat()
        if osd and (osd.is_perf_dump_enabled() or osd.is_perf_record_enabled()):
            log.info("Resetting OSD perf counters for Load Point %s", self.current_lp)
            osd.reset_perf_counters()
        if osd and osd.is_perf_dump_enabled():
            osd.start_periodic_perf_dump(self.current_lp, self.results_dir)

        if self._ganesha_active():
            log.info(
                "Resetting Ganesha perf counters for Load Point %s", self.current_lp
            )
            for host in self.ganesha_manager.ganeshas:
                self.ganesha_manager.reset_ganesha_perf(host)
                if self._ganesha_lockstat_enabled():
                    self.ganesha_manager.reset_lockstat(host)

        if self.client_lockstat.get("enabled") and not self.client_lockstat_started:
            log.info(
                "Starting client lockstat for Load Point %s", self.current_lp
            )
            self.start_client_lockstat()

    def on_loadpoint_end(self):
        if self._ended_for == self.current_lp:
            return
        self._ended_for = self.current_lp
        manager = self.cephfs_manager if self._has_mds() else None
        lp_cfg = self.current_lp_cfg()
        if manager:
            manager.stop_periodic_perf_dump()
        if self.osd_observer:
            self.osd_observer.stop_periodic_perf_dump()
        if manager and manager.is_mds_lockstat_enabled():
            log.info("Dumping MDS lockstat for Load Point %s", self.current_lp)
            manager.dump_lockstat(
                self.current_lp,
                self.results_dir,
                settings=self.perf_settings,
                lp_cfg=lp_cfg,
            )
        if self.osd_observer and self.osd_observer.is_lockstat_enabled():
            log.info("Dumping OSD lockstat for Load Point %s", self.current_lp)
            self.osd_observer.dump_lockstat(
                self.current_lp,
                self.results_dir,
                settings=self.perf_settings,
                lp_cfg=lp_cfg,
            )
        if self.logging_triggered and manager:
            log.info("Stopping MDS logging for Load Point %s", self.current_lp)
            manager.stop_fs_logging(self.current_lp, self.results_dir)
        if manager and self.results_dir:
            log.info("Dumping MDS perf counters for Load Point %s", self.current_lp)
            manager.dump_perf_counters(
                self.current_lp,
                self.results_dir,
                settings=self.perf_settings,
                lp_cfg=lp_cfg,
            )
        if self.osd_observer and self.results_dir and (
            self.osd_observer.is_perf_dump_enabled()
            or self.osd_observer.is_perf_record_enabled()
            or self.osd_observer.is_lockstat_enabled()
        ):
            # One-shot asok dump at the end of the window, even when periodic
            # collection is off. Skip when nothing OSD-related is enabled.
            if self.osd_observer.is_perf_dump_enabled() or self._osd_end_dump():
                log.info(
                    "Dumping OSD perf counters for Load Point %s", self.current_lp
                )
                self.osd_observer.dump_perf_counters(
                    self.current_lp,
                    self.results_dir,
                    settings=self.perf_settings,
                    lp_cfg=lp_cfg,
                )
        self._collect_ganesha()
        if self.ganesha_manager and self.ganesha_manager.should_collect_client_logs():
            log.info(
                "Collecting Ganesha Ceph client logs for Load Point %s",
                self.current_lp,
            )
            self.ganesha_manager.collect_ganesha_client_logs(
                self.current_lp, self.results_dir
            )
        if self.after_loadpoint_end:
            self.after_loadpoint_end(self)
        elif self.ganesha_perf_dumps or self.ganesha_lockstat:
            self.write_ganesha_sidecars()

    def _osd_end_dump(self):
        """End-of-window OSD perf dump follows the periodic gate.

        Lockstat-only runs should not also require a perf dump. Periodic
        ``perf_dump.enabled`` is the switch; a one-shot dump still runs when
        that switch is on (the caller checks ``is_perf_dump_enabled``).
        """
        return False

    def _maybe_start_perf(self, line):
        if not self.run_phase_started:
            return
        if not self._other_perf_started:
            self._start_other_perf()
            self._other_perf_started = True
        if self._mds_perf_started:
            return
        if self.mds_perf_trigger == "immediate" or _is_progress_line(line):
            self._start_mds_perf()
            self._mds_perf_started = True

    def _has_mds(self):
        """True for CephFS managers. Pool and stub managers have no MDS hosts."""
        manager = self.cephfs_manager
        return bool(manager and getattr(manager, "mdss", None))

    def _start_mds_perf(self):
        manager = self.cephfs_manager if self._has_mds() else None
        if not manager or not manager.is_mds_perf_record_enabled():
            return
        log.info("Triggering MDS perf recording for Load Point %s", self.current_lp)
        cfg = role_observability(self.config._config, "mds").get("perf_record") or {}
        self._spawn_perf(
            "mds",
            self.config.mdss,
            cfg,
        )

    def _start_other_perf(self):
        if self._client_perf and self._client_perf.get("enabled"):
            log.info(
                "Triggering client perf recording (%s) for Load Point %s",
                self._client_perf.get("executable"),
                self.current_lp,
            )
            self._spawn_perf(
                self.workload_name,
                self.config.clients,
                self._client_perf,
            )
        osd = self.osd_observer
        if osd and osd.is_perf_record_enabled():
            hosts = osd.hosts()
            if not hosts:
                log.warning(
                    "OSD perf record enabled but no OSD hosts were found; skipping"
                )
            else:
                log.info(
                    "Triggering OSD perf recording for Load Point %s", self.current_lp
                )
                cfg = osd._perf_record_cfg()
                self._spawn_perf("osd", hosts, cfg)
        if self._ganesha_perf_enabled():
            log.info(
                "Triggering Ganesha perf recording for Load Point %s", self.current_lp
            )
            cfg = role_observability(self.config._config, "ganesha").get("perf_record") or {}
            self._spawn_perf("ganesha", self.config.ganeshas, cfg)
        if self.config_section == "elbencho":
            cfg = section_perf_record(
                self.config._config, "elbencho", default_executable="radosgw"
            )
            if cfg.get("enabled"):
                log.info(
                    "Triggering elbencho perf recording for Load Point %s",
                    self.current_lp,
                )
                targets = self.config.rgws or self.config.mons
                self._spawn_perf("elbencho", targets, cfg)

    def _spawn_perf(self, workload_name, hosts, perf_cfg):
        if not hosts:
            log.warning(
                "perf record for %s has no target hosts (load point %s)",
                workload_name,
                self.current_lp,
            )
            return
        lp_cfg = self.current_lp_cfg()
        thread = threading.Thread(
            target=self.runner.execute_perf_record,
            kwargs={
                "workload_name": workload_name,
                "target_nodes": hosts,
                "loadpoint": self.current_lp,
                "results_dir": self.results_dir,
                "settings": self.perf_settings,
                "lp_cfg": lp_cfg,
                "perf_cfg": perf_cfg,
            },
        )
        thread.start()
        self.perf_threads.append(thread)

    def _ganesha_active(self):
        return bool(self.config.ganesha_enabled and self.ganesha_manager)

    def _ganesha_lockstat_enabled(self):
        lockstat = role_observability(self.config._config, "ganesha").get("lockstat") or {}
        return bool(lockstat.get("enabled"))

    def _ganesha_perf_enabled(self):
        if not self.config.ganesha_enabled:
            return False
        return bool(self.config.ganesha_perf_record)

    def _collect_ganesha(self):
        self.ganesha_perf_dumps = {}
        self.ganesha_lockstat = {}
        if not self._ganesha_active() or not self.results_dir:
            return
        log.info("Dumping Ganesha perf counters for Load Point %s", self.current_lp)
        for host in self.ganesha_manager.ganeshas:
            dump = self.ganesha_manager.collect_ganesha_perf_dump(host)
            if dump:
                self.ganesha_perf_dumps[host] = dump
            if self._ganesha_lockstat_enabled():
                lockstat_json = self.ganesha_manager.dump_lockstat(host)
                if lockstat_json:
                    self.ganesha_lockstat[host] = lockstat_json

    def write_ganesha_sidecars(self):
        lp_tag = f"{int(self.current_lp):02d}"
        for host, dump in self.ganesha_perf_dumps.items():
            body = dict(dump)
            if host in self.ganesha_lockstat:
                body["lockstat_results"] = self.ganesha_lockstat[host]
            self._save_json(f"{host}_lp{lp_tag}_ganesha_perf.json", body)
        for host, lockstat_json in self.ganesha_lockstat.items():
            if host in self.ganesha_perf_dumps:
                continue
            self._save_json(
                f"{host}_lp{lp_tag}_ganesha_lockstat.json", lockstat_json
            )

    def _save_json(self, filename, data):
        local_temp = f"/tmp/{filename}"
        with open(local_temp, "w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=4)
        user, host, port = self.executor.get_ssh_details(self.runner.admin)
        subprocess.run(
            [
                "scp",
                "-o",
                "StrictHostKeyChecking=no",
                "-P",
                str(port),
                local_temp,
                f"{user}@{host}:{self.results_dir}/",
            ],
            check=False,
        )
        os.remove(local_temp)

    def _client_lockstat_env(self):
        section = self.config.get(self.config_section) or {}
        env = self.config.get_merged_env_vars(section.get("env_vars"))
        env.pop("CEPH_ARGS", None)
        return env

    def _client_lockstat_paths(self):
        cfg = self.client_lockstat or {}
        path = self.config.expand_env(cfg.get("path") or "ceph-lockstat")
        asok = cfg.get("asok") or "/var/run/ceph/cephfs-tool.asok"
        return path, asok

    def start_client_lockstat(self):
        path, asok = self._client_lockstat_paths()
        env = self._client_lockstat_env()
        for client in self.config.clients:
            log.info("[%s] Starting client lockstat via %s", client, asok)
            self.executor.run_remote(
                client,
                CommonUtils.with_env_exports(f"{path} {asok} start", env),
            )
        self.client_lockstat_started = True

    def reset_client_lockstat(self):
        path, asok = self._client_lockstat_paths()
        env = self._client_lockstat_env()
        for client in self.config.clients:
            log.info("[%s] Resetting client lockstat via %s", client, asok)
            self.executor.run_remote(
                client,
                CommonUtils.with_env_exports(f"{path} {asok} reset", env),
            )

    def dump_client_lockstat(self, phase):
        _path, asok = self._client_lockstat_paths()
        env = self._client_lockstat_env()
        results = {}
        for client in self.config.clients:
            log.info(
                "[%s] Dumping client lockstat (%s phase) via %s", client, phase, asok
            )
            output = self.executor.run_remote(
                client,
                CommonUtils.with_env_exports(
                    f"ceph --admin-daemon {asok} lockstat dump 2>&1", env
                ),
            )
            try:
                results[client] = json.loads(output)
            except json.JSONDecodeError as exc:
                log.warning("[%s] Failed to parse lockstat JSON: %s", client, exc)
                log.info("[%s] lockstat dump output: %r", client, output)
                results[client] = {"raw": output, "parse_error": str(exc)}
        return results


def _is_progress_line(line):
    return "Run " in line and " percent complete" in line
