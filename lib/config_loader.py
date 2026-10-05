"""YAML config composition and observability normalization.

``extends`` loads a single parent (recursively). Dicts deep-merge; lists and
scalars from the child replace the parent. A file with no ``extends`` is
returned unchanged so legacy monoliths keep working.
"""

import copy
import os

import yaml

DEFAULT_PERF_SCRIPT = "/cephfs_perf/perf_record.py"
DEFAULT_FLAMEGRAPH = "/cephfs_perf/FlameGraph"

ROLE_DEFAULT_EXECUTABLE = {
    "mds": "ceph-mds",
    "osd": "ceph-osd",
    "ganesha": "ganesha.nfsd",
}

# Client-process capture. Flat ``<workload>.perf_record_executable`` values
# that do not match these are obsolete server/MDS gates and are ignored.
CLIENT_PERF_EXECUTABLE = {
    "fio": "fio",
    "rbd": "fio",
    "cephfs_tool": "cephfs-tool",
    "mdsbench": "cephfs-mdsbench",
    "rados_bench": "rados",
}


def deep_merge(base, override):
    """Merge mappings. Nested dicts combine; lists and scalars replace."""
    if not isinstance(base, dict) or not isinstance(override, dict):
        return copy.deepcopy(override)
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(value, dict)
        ):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def load_yaml_config(path):
    """Load a YAML file, resolving a single recursive ``extends`` parent."""
    return _load_yaml(os.path.abspath(path), set())


def _load_yaml(path, seen):
    path = os.path.abspath(path)
    if path in seen:
        cycle = " -> ".join(list(seen) + [path])
        raise ValueError(f"Config extends cycle: {cycle}")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    seen.add(path)
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config {path} must be a mapping")
    parent = data.get("extends")
    if not parent:
        data.pop("extends", None)
        return data
    if not isinstance(parent, str):
        raise ValueError(f"{path}: extends must be a path string")
    if os.path.isabs(parent):
        parent_path = parent
    else:
        parent_path = os.path.normpath(os.path.join(os.path.dirname(path), parent))
    base = _load_yaml(parent_path, seen)
    child = {key: value for key, value in data.items() if key != "extends"}
    return deep_merge(base, child)


def normalize_perf_record(parent, default_executable="ceph-mds", default_duration=5):
    """Return a perf_record dict from a nested block or legacy flat keys.

    Accepts either ``parent['perf_record']`` as a mapping
    (``enabled`` / ``script`` / ``executable`` / ``duration``) or the older
    bool plus ``perf_record_script`` / ``perf_record_executable`` /
    ``perf_record_duration`` siblings.
    """
    parent = parent or {}
    raw = parent.get("perf_record")
    if isinstance(raw, dict):
        enabled = bool(raw.get("enabled", False))
        script = (
            raw.get("script")
            or raw.get("perf_record_script")
            or parent.get("perf_record_script")
            or DEFAULT_PERF_SCRIPT
        )
        executable = (
            raw.get("executable")
            or raw.get("perf_record_executable")
            or parent.get("perf_record_executable")
            or default_executable
        )
        duration = raw.get("duration", raw.get("perf_record_duration"))
        if duration is None:
            duration = parent.get("perf_record_duration", default_duration)
        flamegraph = (
            raw.get("flamegraph_path")
            or parent.get("perf_record_flamegraph_path")
            or parent.get("flamegraph_path")
            or DEFAULT_FLAMEGRAPH
        )
        stap = raw.get("stap_script")
        if stap is None:
            stap = parent.get("stap_script") or ""
        return {
            "enabled": enabled,
            "script": script,
            "executable": executable,
            "duration": duration,
            "flamegraph_path": flamegraph,
            "stap_script": stap,
        }

    enabled = bool(raw) if raw is not None else False
    return {
        "enabled": enabled,
        "script": parent.get("perf_record_script", DEFAULT_PERF_SCRIPT),
        "executable": parent.get("perf_record_executable", default_executable),
        "duration": parent.get("perf_record_duration", default_duration),
        "flamegraph_path": parent.get("perf_record_flamegraph_path")
        or parent.get("flamegraph_path")
        or DEFAULT_FLAMEGRAPH,
        "stap_script": parent.get("stap_script") or "",
    }


def overlay_perf_record(base_cfg, raw):
    """Apply an explicit perf_record value on top of an already-normalized dict."""
    out = dict(base_cfg)
    if not isinstance(raw, dict):
        out["enabled"] = bool(raw)
        return out
    if "enabled" in raw:
        out["enabled"] = bool(raw["enabled"])
    for src, dest in (
        ("script", "script"),
        ("executable", "executable"),
        ("duration", "duration"),
        ("flamegraph_path", "flamegraph_path"),
        ("stap_script", "stap_script"),
    ):
        if src in raw and raw[src] is not None:
            out[dest] = raw[src]
    return out


def _as_dict(value):
    return dict(value) if isinstance(value, dict) else {}


def _legacy_role(config, role):
    config = config or {}
    if role == "mds":
        mds = _as_dict(config.get("mds"))
        spec_ls = _as_dict(_as_dict(config.get("specstorage")).get("lockstat"))
        mds_ls = _as_dict(mds.get("lockstat"))
        return {
            "lockstat": deep_merge(spec_ls, mds_ls),
            "perf_record": normalize_perf_record(
                mds, default_executable="ceph-mds", default_duration=5
            ),
            "perf_dump": _as_dict(mds.get("perf_dump")),
            "logging": _as_dict(mds.get("logging")),
        }
    if role == "ganesha":
        ganesha = _as_dict(config.get("ganesha"))
        return {
            "lockstat": _as_dict(ganesha.get("lockstat")),
            "perf_record": normalize_perf_record(
                ganesha, default_executable="ganesha.nfsd", default_duration=5
            ),
        }
    if role == "osd":
        return {
            "lockstat": {},
            "perf_record": normalize_perf_record(
                {}, default_executable="ceph-osd", default_duration=30
            ),
            "perf_dump": {},
        }
    return {}


def role_observability(config, role):
    """Role observability with legacy keys underneath and ``observability.<role>`` on top."""
    legacy = _legacy_role(config, role)
    explicit = _as_dict(_as_dict((config or {}).get("observability")).get(role))
    merged = deep_merge(
        legacy, {key: value for key, value in explicit.items() if key != "perf_record"}
    )
    default_exe = ROLE_DEFAULT_EXECUTABLE.get(role, "ceph-mds")
    default_duration = 5 if role in ("mds", "ganesha") else 30
    base_perf = merged.get("perf_record") or normalize_perf_record(
        {}, default_executable=default_exe, default_duration=default_duration
    )
    if "perf_record" in explicit:
        merged["perf_record"] = overlay_perf_record(base_perf, explicit["perf_record"])
    else:
        merged["perf_record"] = base_perf
    return merged


def _section(config, workload):
    return _as_dict((config or {}).get(workload))


def client_perf_record(config, workload, default_executable=None, default_duration=30):
    """Process-local client perf record for a workload section.

    Prefers ``<workload>.client.perf_record``. Legacy flat keys are honored
    only when ``perf_record_executable`` matches the client binary. A flat
    gate aimed at another daemon (historical ``fio`` → ``ganesha.nfsd``) is
    ignored so it cannot retarget client capture.
    """
    section = _section(config, workload)
    executable = default_executable or CLIENT_PERF_EXECUTABLE.get(workload, "fio")
    client = _as_dict(section.get("client"))
    if "perf_record" in client:
        synthetic = {
            "perf_record": client.get("perf_record"),
            "perf_record_script": section.get("perf_record_script"),
            "perf_record_executable": section.get("perf_record_executable"),
            "perf_record_duration": section.get("perf_record_duration"),
            "flamegraph_path": section.get("flamegraph_path"),
            "stap_script": section.get("stap_script"),
            "perf_record_flamegraph_path": section.get("perf_record_flamegraph_path"),
        }
        return normalize_perf_record(
            synthetic,
            default_executable=executable,
            default_duration=default_duration,
        )
    cfg = normalize_perf_record(
        section, default_executable=executable, default_duration=default_duration
    )
    if "perf_record" not in section:
        cfg["enabled"] = False
        return cfg
    flat_exe = section.get("perf_record_executable")
    if flat_exe and os.path.basename(str(flat_exe)) != os.path.basename(str(executable)):
        cfg["enabled"] = False
    return cfg


def section_perf_record(config, workload, default_executable, default_duration=30):
    """Workload-level perf record, preferring ``<workload>.server.perf_record``."""
    section = _section(config, workload)
    server = _as_dict(section.get("server"))
    if "perf_record" in server:
        synthetic = {
            "perf_record": server.get("perf_record"),
            "flamegraph_path": section.get("flamegraph_path"),
            "stap_script": section.get("stap_script"),
            "perf_record_script": section.get("perf_record_script"),
            "perf_record_executable": section.get("perf_record_executable"),
            "perf_record_duration": section.get("perf_record_duration"),
            "perf_record_flamegraph_path": section.get("perf_record_flamegraph_path"),
        }
        return normalize_perf_record(
            synthetic,
            default_executable=default_executable,
            default_duration=default_duration,
        )
    return normalize_perf_record(
        section,
        default_executable=default_executable,
        default_duration=default_duration,
    )


def client_lockstat(config, workload):
    """``<workload>.client.lockstat``, else the legacy section-level lockstat map."""
    section = _section(config, workload)
    client = _as_dict(section.get("client"))
    if isinstance(client.get("lockstat"), dict):
        return dict(client["lockstat"])
    if isinstance(section.get("lockstat"), dict):
        return dict(section["lockstat"])
    return {}


def workload_perf_dump(config, workload):
    """``<workload>.client.perf_dump``, else the legacy section-level perf_dump map."""
    section = _section(config, workload)
    client = _as_dict(section.get("client"))
    if isinstance(client.get("perf_dump"), dict):
        return dict(client["perf_dump"])
    if isinstance(section.get("perf_dump"), dict):
        return dict(section["perf_dump"])
    return {}
