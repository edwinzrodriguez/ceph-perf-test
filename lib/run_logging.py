"""Framework run logging: one console stream and one durable transcript.

This is the orchestrator logger (``run_logging`` in YAML). It is not Ceph
daemon debug (``observability.mds.logging``) and not the remote driver
stdout protocol.
"""

import logging
import os
import sys
from datetime import datetime, timezone

LOGGER_NAME = "ceph_perf"

_RUN_LOG_PATH = None

_WORKLOAD_SECTIONS = (
    "fio",
    "specstorage",
    "cephfs_tool",
    "mdsbench",
    "rados_bench",
    "rbd",
    "elbencho",
)


class RunLogger:
    """logging.Logger facade that tolerates print-style ``end`` / ``flush`` kwargs.

    Controller call sites that used to print with ``end=""`` can pass the
    same text to ``log.info`` without the logging module rejecting ``end``.
    """

    def __init__(self, name):
        self._logger = logging.getLogger(name)

    def info(self, msg="", *args, **kwargs):
        self._emit(logging.INFO, msg, args, kwargs)

    def warning(self, msg="", *args, **kwargs):
        self._emit(logging.WARNING, msg, args, kwargs)

    def error(self, msg="", *args, **kwargs):
        self._emit(logging.ERROR, msg, args, kwargs)

    def exception(self, msg="", *args, **kwargs):
        kwargs["exc_info"] = kwargs.get("exc_info", True)
        self._emit(logging.ERROR, msg, args, kwargs)

    def _emit(self, level, msg, args, kwargs):
        kwargs.pop("end", None)
        kwargs.pop("flush", None)
        kwargs.pop("file", None)
        kwargs.pop("sep", None)
        exc_info = kwargs.pop("exc_info", None)
        text = msg
        if args:
            try:
                text = msg % args
            except (TypeError, ValueError):
                text = " ".join([str(msg)] + [str(arg) for arg in args])
        self._logger.log(level, str(text).rstrip("\n"), exc_info=exc_info)


def get_logger(name=None):
    """Return a logger under the ``ceph_perf`` hierarchy."""
    if not name or name == "__main__":
        full = LOGGER_NAME
    elif name.startswith(LOGGER_NAME + ".") or name == LOGGER_NAME:
        full = name
    else:
        full = f"{LOGGER_NAME}.{name}"
    return RunLogger(full)


def run_log_path():
    return _RUN_LOG_PATH


def _config_get(config, key, default=None):
    if config is None:
        return default
    getter = getattr(config, "get", None)
    if callable(getter):
        return getter(key, default)
    return default


def default_run_log_path(config):
    """``<first workload results_base_dir>/run_<utc>.log``, else ``./run_logs``."""
    base = None
    for section_name in _WORKLOAD_SECTIONS:
        section = _config_get(config, section_name) or {}
        if isinstance(section, dict) and section.get("results_base_dir"):
            base = section["results_base_dir"]
            break
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    filename = f"run_{ts}.log"
    if base:
        try:
            os.makedirs(base, exist_ok=True)
            probe = os.path.join(base, filename)
            # Confirm the directory is writable from this process.
            with open(probe, "a", encoding="utf-8"):
                pass
            return probe
        except OSError:
            pass
    fallback = os.path.join(os.getcwd(), "run_logs")
    os.makedirs(fallback, exist_ok=True)
    return os.path.join(fallback, filename)


def setup_run_logging(config):
    """Configure console and file handlers from the ``run_logging`` block.

    Called once after the YAML (including ``extends``) is loaded and before
    inventory construction, so later controller log records share one file.
    """
    global _RUN_LOG_PATH
    cfg = _config_get(config, "run_logging") or {}
    if not isinstance(cfg, dict):
        cfg = {}
    level_name = str(cfg.get("level", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)
    console = bool(cfg.get("console", True))
    file_enabled = bool(cfg.get("file", True))

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    logger.propagate = False
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    if console:
        stream = logging.StreamHandler(sys.stdout)
        stream.setLevel(level)
        stream.setFormatter(formatter)
        logger.addHandler(stream)

    path = None
    if file_enabled:
        path = cfg.get("file_path") or default_run_log_path(config)
        directory = os.path.dirname(path) or "."
        os.makedirs(directory, exist_ok=True)
        file_handler = logging.FileHandler(path, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    _RUN_LOG_PATH = path
    if path:
        get_logger("run_logging").info("Run log: %s", path)
    return path


def publish_run_log(executor, admin, results_dir):
    """Copy the run transcript into an iteration results directory on admin."""
    path = _RUN_LOG_PATH
    if not path or not admin or not results_dir or not os.path.isfile(path):
        return
    log = get_logger("run_logging")
    try:
        import subprocess

        user, host, port = executor.get_ssh_details(admin)
        dest_name = os.path.basename(path)
        remote = f"{results_dir}/{dest_name}"
        subprocess.run(
            [
                "scp",
                "-o",
                "StrictHostKeyChecking=no",
                "-P",
                str(port),
                path,
                f"{user}@{host}:{remote}",
            ],
            check=False,
        )
        log.info("Copied run log to %s:%s", admin, remote)
    except Exception as exc:
        log.warning("Could not copy run log into %s: %s", results_dir, exc)
