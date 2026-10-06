# Generated Perf Data Files — Cheat Sheet

Quick reference for consumers of `ceph-perf-test` result directories: what files can appear, how they are named, and what each type contains.

Not every file is produced on every run. Presence depends on the workload and which optional collectors are enabled (MDS logging, lockstat, perf record, Ganesha/Samba/FUSE, periodic dumps, etc.).

---

## Results directory

Each test iteration writes into a timestamped directory under the workload’s `results_base_dir`:

```text
<results_base_dir>/<UTC-timestamp>_<workload>_<fs>-x<Nfs>-c<Nclients>[-m<mounts>]_<mds_settings>[_<mount>][_<ganesha>][_<samba>][_<rgw>]/
```

| Piece | Meaning |
|-------|---------|
| `UTC-timestamp` | `YYYYMMDD-HHMMSS-ffffff` |
| `workload` | Runner name: `cephfs_tool`, `mdsbench`, `fio`, `sfs2020`, `rados_bench`, `rbd`, `elbencho` |
| `fs-xN-cN` | Filesystem name, number of filesystems, number of clients |
| `-mN` | Mounts per filesystem (fio / sfs2020) |
| MDS / protocol suffixes | Compact encoding of active settings (same short codes as in filenames) |

Default bases (overridable in YAML):

| Workload | Default `results_base_dir` |
|----------|----------------------------|
| `cephfs_tool` | `/tmp/cephfs_tool_results` |
| `mdsbench` | `/tmp/mdsbench_results` |
| `fio` | `/tmp/fio_results` |
| `sfs2020` | `/cephfs_perf/sfs2020/results` |
| `rados_bench` | `/tmp/rados_bench_results` |
| `rbd` | `/tmp/rbd_results` |
| `elbencho` | `/cephfs_perf/results` |

Load-point numbers in filenames are zero-padded two digits: `lp01`, `lp02`, …

---

## Core naming pattern

Most JSON/text artifacts use:

```text
<workload>_<output_type>_<host_or_client>_lp<NN>_<encoded_settings>.<ext>
```

| Field | Role |
|-------|------|
| `workload` | Source of the file (`cephfs_tool`, `fio`, `mds.<fs>`, `ceph_fuse`, `mds` / `ganesha` for perf-record, …) |
| `output_type` | Kind of data (see below) |
| `host_or_client` | Inventory hostname, mount label, MDS id, or bucket name (elbencho) |
| `lpNN` | Load point index |
| `encoded_settings` | Compact parameter fingerprint for that run |

When settings are not available at dump time, a shorter form is used:

```text
<target>_<output_type>[_<phase>]_<host>_lp<NN>.<ext>
```

### Encoded settings (short codes)

Settings are abbreviated and joined. Global / MDS keys use `-`; loadpoint keys use `_`.

Example fragment:

```text
mds_cache_memory_limit16GiB_s5GiB_t32_oc1_ocs16GiB_bs4MiB_mw8
```

Boolean-ish values are often encoded as `0`/`1`. Sizes use SI/IEC suffixes (`16GiB`, `4MiB`).

See [Appendix A](#appendix-a-encoded-short-names) for the full short-code ↔ human-readable map.

---

## File types

### 1. Workload result JSON (primary metrics)

```text
<workload>_result_<client>_lp<NN>_<encoded>.json
```

| Workload | `workload` token | Client field notes |
|----------|------------------|--------------------|
| cephfs-tool | `cephfs_tool` | Client hostname |
| mdsbench | `mdsbench` | `<host>_c<client_id>` |
| fio | `fio` | Client hostname |
| rados bench | `rados_bench` | Client hostname |
| rbd | `rbd` | Client hostname |
| elbencho | `elbencho` | Bucket name |
| SPECstorage | see §6 | Summary XML + JSON, not this pattern |

Typical contents: throughput/IOPS (or SPECstorage metrics), `test_parameters`, optional embedded `lockstat_results` / `test_results_summary`.

---

### 2. Client / tool perf dumps

Admin-socket `perf dump` at end of load point (or while the client is still running for RBD).

```text
<workload>_perf_dump_<client>_lp<NN>_<encoded>.json
```

| Source | Pattern |
|--------|---------|
| cephfs-tool | `cephfs_tool_perf_dump_<client>_lp…json` |
| RBD client | `rbd_perf_dump_<host>_lp…json` |
| ceph-fuse | `ceph_fuse_perf_dump_<mount_label>_lp…json` (or short form without encoded settings) |

---

### 3. MDS perf dumps (end of load point)

One pair per MDS daemon:

```text
mds.<mds_id>_perf_dump_<host>_lp<NN>[_<encoded>].json
mds.<mds_id>_perf_histogram_dump_mds_<host>_lp<NN>[_<encoded>].json
```

- `perf_dump` — full counter dump  
- `perf_histogram_dump_mds` — MDS histogram dump  

Optional phase suffix on `output_type`: `perf_dump_<phase>`.

---

### 4. Periodic MDS perf dumps (time series)

When `mds.perf_dump.enabled` is set, samples land in a subdirectory:

```text
<results_dir>/lp<NN>/
  <host>_<UTC-timestamp>_perf_dump.json
  <host>_<UTC-timestamp>_perf_histogram_dump_mds.json
```

Timestamps look like `20261006-180012-123456`. Interval is configured in YAML (default often 60s).

---

### 5. Lockstat

Text dumps from `ceph-lockstat` (MDS) or related tooling:

```text
mds.<fs>_lockstat[_<phase>]_<host>_lp<NN>[_<encoded>].txt
```

- With settings: full `get_workload_base_name` form ending in `.txt`  
- Without: `mds.<fs>_lockstat[_phase]_<host>_lpNN.txt`  
- Phases (e.g. cephfs-tool read/write): `lockstat_write`, `lockstat_read`  

Client-side cephfs-tool lockstat is often **injected into** the result JSON (`lockstat_results`) rather than written as a separate file. Ganesha lockstat may be nested inside Ganesha perf JSON the same way.

---

### 6. SPECstorage 2020 (sfs2020)

Run name:

```text
<timestamp>_<encoded_options>
```

Primary summaries in the results directory:

```text
sfssum_<run_name>.xml
sfssum_<run_name>.json
```

JSON includes `runs[]` metrics, `test_parameters`, and `test_results_summary`. SPECstorage may also leave its own workload artifacts under the results dir (tool-specific).

---

### 7. Ganesha

| File | When |
|------|------|
| `ganesha_perf_dump_<host>_lpNN.json` | fio runner |
| `<host>_lpNN_ganesha_perf.json` | sfs2020 runner (may include `lockstat_results`) |
| `ganesha_ceph_client_<host>_lpNN.log` | Client log collection enabled |
| `ganesha_config_diff_<host>.json` | Written at Ganesha provision time |

---

### 8. ceph-fuse client logs

When fuse logging is enabled:

```text
ceph_fuse_<mount_label>_lpNN.log
```

---

### 9. MDS daemon logs

**Copied into results** (non-continuous logging):

| Manager | Destination name |
|---------|------------------|
| systemd | `<host>_lpNN_mds.<mds_id>.log` |
| cephadm | `<host>_lpNN_<daemon_name>.log` (e.g. `ceph-mds.…`) |

**Continuous logging** (`mds.logging.continuous: true`): logs are rotated **on the MDS host** (not copied into the results dir):

```text
/var/log/ceph/ceph-mds.<mds_id>_lpNN.log.gz
```

(path may vary with `mds.log_dir` / cephadm layout). Memory-buffer mode may `log dump` into the live log before rotate/collect.

---

### 10. `perf record` / flamegraphs / SystemTap

When `perf_record` is enabled for MDS, Ganesha, or a workload section:

```text
<workload>_perf_record_<host>_lpNN_<encoded>_perf.data
<workload>_perf_record_<host>_lpNN_<encoded>_perf_report.txt
<workload>_perf_record_<host>_lpNN_<encoded>_perf_script.txt
<workload>_perf_record_<host>_lpNN_<encoded>.svg          # flamegraph (if FlameGraph available)
<workload>_perf_record_<host>_lpNN_<encoded>_stap_trace.txt  # if stap_script set
```

- MDS-targeted captures use workload token `mds`  
- Ganesha-targeted captures use `ganesha`  
- Fallback (no workload/options): `<host>_lpNN_<service_id>_perf*`  

Files are gathered from `/tmp` on the target into the results directory.

---

### 11. Journal logs (failure / end-of-run helpers)

```text
journalctl_<host>.log
```

Last ~5 minutes of `journalctl` from the host. Often collected on workload failure or by helper scripts; not every successful path produces these.

---

### 12. Config diffs (provisioning)

```text
ganesha_config_diff_<host>.json
```

Snapshot of Ganesha/Ceph client config vs defaults at provision time (when `results_dir` is passed into provision).

---

## Quick “what am I looking at?” guide

| If the name contains… | It is… |
|-----------------------|--------|
| `_result_` | Workload outcome / metrics for that client & load point |
| `_perf_dump_` (and not under `lpNN/`) | End-of-LP counter dump |
| `_perf_histogram_dump_mds_` | MDS histogram dump |
| `lpNN/<host>_<timestamp>_perf_` | Periodic in-run MDS sample |
| `_lockstat_` | Lock contention / lockstat text |
| `_perf_record_` + `_perf.data` | Raw Linux perf capture |
| `_perf_report.txt` / `_perf_script.txt` / `.svg` | Human / flamegraph views of perf |
| `sfssum_` | SPECstorage summary |
| `ganesha_perf` / `ganesha_perf_dump` | NFS-Ganesha counters |
| `ganesha_ceph_client_` / `ceph_fuse_` + `.log` | Client daemon logs |
| `*_lpNN_mds.` / `*_lpNN_ceph-mds` + `.log` | MDS logs copied into results |
| `journalctl_` | Host journal snippet |
| `*_config_diff_` | Config-vs-default snapshot |

---

## Optional vs always-on

| Artifact | Typical gate |
|----------|----------------|
| Result JSON | Always for successful load points |
| Client/tool `perf_dump` | Workload supports it (cephfs-tool, rbd, fuse) |
| MDS end-of-LP dumps | MDS present + dump path in runner |
| Periodic MDS dumps | `mds.perf_dump.enabled` |
| Lockstat `.txt` | `mds.lockstat` / `*.lockstat.enabled` |
| MDS / fuse / Ganesha logs | Corresponding `logging` / log-level settings |
| Continuous MDS `.gz` on host | `mds.logging.continuous` |
| `perf record` suite | `perf_record` / `mds.perf_record` / `ganesha.perf_record` |
| Ganesha / Samba / RGW suffixes | Those protocols enabled |

---

## Tips for consumers

1. **Filter by load point**: glob `*_lp03_*` (or `lp03/` for periodic dumps).  
2. **Pair metrics with counters**: same `lpNN` + overlapping host/client + similar encoded suffix.  
3. **Prefer `sfssum_*.json`** over XML for SPECstorage automation.  
4. **Encoded suffixes can differ slightly** between file kinds if dumps omit `settings`/`lp_cfg`; match on workload + host + `lpNN` first.  
5. **Continuous MDS logs** may only exist on the MDS nodes under `/var/log/ceph/`, not in the shared results tree.

---

## Appendix A: Encoded short names

Complete mapping used by `CommonUtils.get_short_name()` when building directory and filename suffixes. Codes are listed alphabetically by short name.

Unmapped parameter names fall back to the human-readable string with spaces and `/` replaced by `_` (for example, a raw YAML key that never went through the name map).

| Short | Human-readable name |
|-------|---------------------|
| `bs` | Block Size |
| `buf` | Buffered I/O |
| `c` | Clients |
| `cs` | Create Serialize |
| `cta` | CephFS Tool Async |
| `ctqd` | CephFS Tool Queue Depth |
| `d` | Direct I/O |
| `de` | MDS Dispatch Engine |
| `dur` | Duration |
| `fe` | Fsync Every |
| `fs` | Filesystem Name |
| `ft` | Fio Threads |
| `gas` | Ganesha Async |
| `gcbp` | Ganesha Ceph Binary Path |
| `gcftr` | Ganesha Client Fsync To Rados |
| `ge` | Ganesha Enabled |
| `gkp` | Ganesha Keyring Path |
| `gmw` | Ganesha Msgr Workers |
| `goc` | Ganesha Client Object Cache |
| `gocs` | Ganesha Client Object Cache Size |
| `goss` | Ganesha Objecter Session Strand |
| `gr` | GTOD Reduce |
| `grpcmax` | Ganesha RPC IOQ Thread Max |
| `grpcmin` | Ganesha RPC IOQ Thread Min |
| `gsdo` | Ganesha Sync Data Only |
| `gsts` | Ganesha Slot Table Size |
| `guid` | Ganesha User ID |
| `gum` | Ganesha Umask |
| `gwt` | Ganesha Worker Threads |
| `gzc` | Ganesha Zero Copy |
| `iod` | I/O Depth |
| `ioe` | I/O Engine |
| `m` | MDS Cache Memory Limit |
| `maxos` | Max Object Size |
| `minos` | Min Object Size |
| `mode` | Mode |
| `mpf` | Mounts per Filesystem |
| `mw` | Msgr Workers |
| `nf` | Number of Filesystems |
| `oc` | Client Object Cache |
| `ocs` | Client Object Cache Size |
| `ops` | Ops Per Sec |
| `rcph` | RGW Count Per Host |
| `re` | RGW Enabled |
| `rfp` | RGW Frontend Port |
| `rt` | Ramp Time / RGW Type |
| `rw` | Read/Write Pattern |
| `rwmixread` | Read/Write Mix (Read %) |
| `s` | File Size |
| `scbp` | Samba Ceph Binary Path |
| `scl` | Samba Clustering |
| `scll` | Samba Client Log Level |
| `scv` | Samba Ceph VFS |
| `se` | Samba Enabled |
| `sfll` | Samba Finisher Log Level |
| `skp` | Samba Keyring Path |
| `smw` | Samba Msgr Workers |
| `socs` | Samba Client Object Cache Size |
| `st` | Samba Type |
| `suid` | Samba User ID |
| `swg` | Samba Workgroup |
| `t` | Threads |
| `wr` | Workload Runner |
| `wu` | Warmup |

### Same short code, different meanings

`rt` is defined for both **Ramp Time** and **RGW Type** in the short-name map. In practice, which one appears depends on which setting is active in that filename segment; disambiguate from surrounding codes (`rfp`/`rcph` for RGW vs I/O-oriented codes for fio ramp time).

### Grouped by area

| Area | Short codes |
|------|-------------|
| MDS / FS topology | `m`, `de`, `fs`, `nf`, `mpf` |
| Workload / I/O | `s`, `t`, `c`, `mode`, `ops`, `wu`, `bs`, `iod`, `rw`, `rwmixread`, `ioe`, `d`, `buf`, `cs`, `dur`, `rt` (ramp), `gr`, `mw`, `ft`, `minos`, `maxos`, `wr` |
| Client OC | `oc`, `ocs` |
| cephfs-tool | `cta`, `ctqd`, `fe` |
| Ganesha | `ge`, `gwt`, `gum`, `goc`, `gsdo`, `gcftr`, `gas`, `gzc`, `gocs`, `gmw`, `goss`, `gsts`, `grpcmin`, `grpcmax`, `guid`, `gkp`, `gcbp` |
| Samba | `se`, `st`, `scv`, `scl`, `swg`, `socs`, `smw`, `scll`, `sfll`, `suid`, `skp`, `scbp` |
| RGW | `re`, `rt` (type), `rcph`, `rfp` |
