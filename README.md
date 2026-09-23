# CephFS Performance Test Framework

A structured framework for running performance benchmarks on CephFS using various workload drivers, supporting both kernel mounts and NFS-Ganesha.

## Theory of Operation

The framework operates on a **Test Matrix** principle. It iterates through combinations of MDS settings and Ganesha settings, automatically provisioning the environment for each iteration.

1.  **Configuration**: Defined in a YAML file (e.g., `MDSConfigurationSettings.yml`).
2.  **Matrix Expansion**: If any setting in `mds_settings` or `ganesha` is a list, the framework calculates the Cartesian product of all combinations.
3.  **Iteration Lifecycle**:
    -   Unmount clients.
    -   Provision Grafana/Prometheus monitoring (if enabled; once per run, before the matrix).
    -   Rebuild/Reset CephFS (optional, based on settings).
    -   Apply MDS configurations.
    -   Provision Ganesha (if enabled).
    -   Mount clients (Kernel or NFS).
    -   Expand and run Workload Loadpoints.
    -   Collect results and performance traces.

## Usage

```bash
./cephfs_fio_runner.py <config.yaml> [<ansible_inventory>]
./cephfs_fio_runner.py <config.yaml> --grafana systemd
```

Workload-specific entry points: `cephfs_fio_runner.py`, `cephfs_sfs2020_runner.py`, `cephfs_tool_bench_runner.py`, `cephfs_rados_bench_runner.py`, `cephfs_rbd_runner.py`, `cephfs_elbencho_runner.py`, or `cephfs_all_bench_runner.py` to run the full suite.

---

## Configuration Reference

### Top-Level Keys

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `fs_name` | string | required | Name of the CephFS filesystem |
| `num_filesystems` | int | `1` | Number of filesystems to create |
| `fs_manager_type` | string | `CephFSManager` | Manager class: `CephFSCephadmManager` (or legacy `CephFSManager`) deploys MDS via `ceph orch`; `CephFSSystemdManager` runs local `ceph-mds`; also `StubFSManager`, `CephPoolManager` |
| `mount_manager_type` | string | `MountKernelManager` | Mount handler (`MountKernelManager`, `MountFuseManager`, `MountNfsManager`, `StubMountManager`) |
| `mds_yaml_path` | string | `/cephfs_perf/mds.yaml` | Path to MDS cephadm spec file (cephadm only) |

---

### `mds`

Settings for local `ceph-mds` processes when `fs_manager_type` is `CephFSSystemdManager`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `binary_path` | string | `/usr/local/bin/ceph-mds` | Path to the `ceph-mds` executable |
| `ceph_binary_path` | string | `ceph` | Path to the `ceph` CLI (auth, config). Use `${CEPH_INSTALL_PREFIX}/bin/ceph` or omit to rely on PATH from `env_vars` |
| `data_dir` | string | `/var/lib/ceph/mds` | Parent directory for MDS keyrings (`ceph-<id>/keyring`) |
| `log_dir` | string | `/var/log/ceph` | Directory for MDS log files |
| `run_dir` | string | `/var/run/ceph` | Directory for PID and admin-socket files |
| `env_vars` | map | `{}` | Environment variables exported when starting `ceph-mds` (merged over defaults `ENABLE_LOCKSTAT` and `CEPH_CONF`) |
| `mds_yaml_path` | string | `/cephfs_perf/mds.yaml` | Path to MDS cephadm spec file |
| `conf_settings` | list | `[]` | Additional `mds_settings` keys written to the MDS-only config file instead of `ceph config set mds` |
| `conf_path` | string | `/etc/ceph/mds-settings.conf` | Path to the MDS-only config file (a copy of cluster `ceph.conf` with MDS overrides merged in; not read by mon) |
| `dispatch_engine_via` | string | `ceph.conf` | How to apply `mds_dispatch_engine`: `ceph.conf` (default, uses `conf_path`) or `ceph-config`. Use `ceph-config` only when monitors know the option (e.g. vstart / uniform wip builds) |

---

### `ceph`

Connection details for the Ceph cluster.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `conf` | string | `/etc/ceph/ceph.conf` | Path to `ceph.conf` |
| `keyring` | string | | Path to Ceph keyring file |
| `user_id` | string | `admin` | Ceph client user ID |
| `fsid` | string | | Cluster FSID (UUID) |

---

### `grafana`

Controls deployment of the Ceph monitoring stack (Grafana, Prometheus, ceph-exporter, node-exporter). When enabled, the benchmark runner provisions monitoring **once before the test matrix** and tears it down after the run completes.

Host preparation (firewall ports, podman) is handled separately by Ansible via `ceph-monitoring-host-prep.yml`, which is imported from `cephfs-sfs-config.yml`.

#### Core

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Enable Grafana/Prometheus monitoring |
| `type` | string | `cephadm` or `systemd` | Deployment type. Defaults to `systemd` when `fs_manager_type` is `CephFSSystemdManager`, otherwise `cephadm` |
| `exporter_prio_limit` | int | `5` | ceph-exporter perf counter priority threshold. `0` exports all counters |
| `yaml_path` | string | `/cephfs_perf/monitoring.yaml` | Remote path for the cephadm monitoring spec (cephadm only) |
| `ceph_binary_path` | string | `${CEPH_INSTALL_PREFIX}/bin/ceph` | Path to the `ceph` CLI |
| `exporter_binary_path` | string | `${CEPH_INSTALL_PREFIX}/bin/ceph-exporter` | Path to `ceph-exporter` (systemd only) |
| `credentials_file` | string | `ibm-credentials.env` | Path to registry credentials file (see below) |
| `registry_list` | list | IBM/quay defaults | Registry URLs and credential key references for `podman login` |

#### Registry Authentication

Upstream monitoring images on quay.io require `podman login` before pulls. The benchmark runner logs in automatically on each `grafanas` host using `ibm-credentials.env` (same `credentials_file:KEY` reference format as ceph-linode).

Default `registry_list` (credential values use `credentials_file:KEY` references):

| Registry | Username key | Password key |
|----------|--------------|--------------|
| `quay.io` | `QUAY_IO_USERNAME` | `QUAY_IO_PASSWORD` |
| `quay.ceph.io` | `QUAY_CEPH_IO_USERNAME` | `QUAY_CEPH_IO_PASSWORD` |

Default container images match the upstream Ceph cephadm monitoring stack:

| Image | Default |
|-------|---------|
| Grafana | `quay.io/ceph/grafana:12.3.1` |
| Prometheus | `quay.io/prometheus/prometheus:v3.6.0` |

#### Ports

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `port` | int | `3000` | Grafana HTTP port |
| `prometheus_port` | int | `9095` | Prometheus HTTP port |
| `exporter_port` | int | `9926` | ceph-exporter HTTP port |
| `mgr_prometheus_port` | int | `9283` | mgr/prometheus module port |

#### Grafana (cephadm)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `anonymous_access` | bool | `true` | Allow anonymous Grafana access |
| `timezone` | string | `UTC` | Grafana UI timezone (`UTC`, an IANA name (such as America/New_York), or `browser`). `UTC` matches Ceph daemon log timestamps. Applied to the systemd Grafana container. |
| `protocol` | string | `http` | Grafana protocol (`http` or `https`) |
| `ssl` | bool | `false` | Enable TLS for Grafana |

#### Container Images (systemd)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `image` | string | `quay.io/ceph/grafana:12.3.1` | Grafana container image |
| `prometheus_image` | string | `quay.io/prometheus/prometheus:v3.6.0` | Prometheus container image |

#### Environment Variables

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `env_vars` | dict | `{}` | Extra environment variables for `ceph` CLI invocations during monitoring setup |

#### Deployment Behavior

**`type: cephadm`** — deploys via `ceph orch apply`:

- `node-exporter` on all hosts (`host_pattern: *`)
- `ceph-exporter` on all hosts with `prio_limit` from `exporter_prio_limit`
- `prometheus` and `grafana` on hosts in the `grafanas` inventory group

**`type: systemd`** — for non-cephadm MDS clusters:

- Enables the mgr `prometheus` module
- Starts `ceph-exporter` as a local process on mon/mgr/mds/osd hosts
- Runs Prometheus and Grafana as podman containers on `grafanas` hosts

#### Inventory

Add a `grafanas` group to the inventory (same pattern as `ganeshas` or `sambas`) for hosts that run Grafana and Prometheus. If omitted, the first `mons` host is used.

```yaml
inventory:
  grafanas:
    mon-000:
      ansible_ssh_host: 169.63.188.95
      ansible_ssh_user: root
      private_ip: 10.241.64.69
```

#### Example Configuration

```yaml
grafana:
  enabled: true
  type: "systemd"
  exporter_prio_limit: 0
  port: 3000
  prometheus_port: 9095
  exporter_port: 9926
  mgr_prometheus_port: 9283
  anonymous_access: true
  timezone: UTC
  protocol: http
  ssl: false
  ceph_binary_path: "${CEPH_INSTALL_PREFIX}/bin/ceph"
  exporter_binary_path: "${CEPH_INSTALL_PREFIX}/bin/ceph-exporter"
```

#### Monitoring Setup and Access

1. **Host prep** (once per cluster):

```bash
ansible-playbook -i <inventory> cephfs-sfs-config.yml
```

This runs `ceph-monitoring-host-prep.yml`, which opens firewall ports (3000, 9095, 9283, 9926, 9100), installs podman on monitoring hosts, and creates `/etc/ceph/monitoring`.

2. **Run benchmarks with monitoring**:

```bash
./cephfs_fio_runner.py MDSConfigurationSettings.yml
# or override deployment type:
./cephfs_fio_runner.py MDSConfigurationSettings.yml --grafana systemd
```

3. **Access dashboards** (default ports on the `grafanas` host):

| Service | URL |
|---------|-----|
| Grafana | `http://<grafana-host>:3000` |
| Prometheus | `http://<grafana-host>:9095` |
| mgr/prometheus | `http://<mgr-host>:9283/metrics` |
| ceph-exporter | `http://<daemon-host>:9926/metrics` |

Workload runners print load-point markers (e.g. `Detected Starting tests... Load Point: N/T (P% done)`) that can be correlated with Grafana time series during a run.

---

### `ganesha`

Controls NFS-Ganesha deployment. Settings that are lists are expanded across the test matrix.

#### Core

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Enable NFS Ganesha |
| `type` | string | `cephadm` | Deployment type (`systemd` or `cephadm`) |
| `service_id` | string | `ganesha` | Service identifier (cephadm only) |
| `binary_path` | string | `/usr/local/ceph/bin/ganesha.nfsd` | Path to `ganesha.nfsd` executable (systemd only) |
| `ceph_binary_path` | string | `/usr/bin/ceph` | Path to the `ceph` CLI |
| `pid_path` | string | `/var/run/ganesha.pid` | PID file location (systemd only) |
| `config_path` | string | `/etc/ganesha/ganesha.conf` | Ganesha config path (systemd only) |
| `ganesha_yaml_path` | string | `/cephfs_perf/ganesha.yaml` | Cephadm spec YAML path (cephadm only) |

#### Authentication

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `user_id` | string | inherits `ceph.user_id` | Ganesha client user ID |
| `keyring_path` | string | inherits `ceph.keyring` | Keyring path for Ganesha |

#### FSAL_CEPH Tuning (support lists for matrix sweep)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `client_oc` | bool or list | | Enable/disable client object cache |
| `client_oc_size` | string or list | | Object cache size (e.g., `16GiB`, `1GiB`) |
| `syncdataonly` | bool or list | | Enable/disable CEPH FSAL `syncdataonly` |
| `client_fsync_to_rados` | bool or list | | Enable/disable CEPH FSAL `client_fsync_to_rados` |
| `async` | bool or list | | Enable async FSAL operations |
| `zerocopy` | bool or list | | Enable zero-copy I/O |
| `umask` | int | | File creation umask |

#### Threading & Performance

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker_threads` | int | | Number of Ganesha 9P worker threads |
| `msgr_workers` | int | | Ceph messenger worker threads (`ms_async_op_threads`) |
| `slot_table_size` | int or list | | NFSv4.1 session slot table size (`NFS_Core_Param.slot_table_size`; max 1024) |
| `rpc_ioq_thrdmin` | int | | RPC I/O queue minimum threads |
| `rpc_ioq_thrdmax` | int or list | | RPC I/O queue maximum threads |

#### Logging

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `log_level` | string | | When set, writes a `LOG {}` block to `ganesha.conf` with per-component levels. NFS_V4 and FSAL are set to this level; other components use fixed levels (DEBUG for DISPATCH/SESSIONS/CLIENTID/STATE, INFO/EVENT for noisy subsystems). Disables the `-F -L STDOUT -N` command-line flags. Example values: `NIV_DEBUG`, `NIV_INFO`, `NIV_EVENT`. |
| `client_log_level` | int | `1` | Ceph client `debug_client` level written to the per-host `ceph.conf`. When this key or `finisher_log_level` is set, the client log file is collected to the results directory at the end of each load point. |
| `finisher_log_level` | int | | Ceph client `debug_finisher` level written to the per-host `ceph.conf`. When this key or `client_log_level` is set, the client log file is collected to the results directory at the end of each load point. |

#### Environment Variables

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `env_vars` | dict | `{}` | Extra environment variables for `ganesha.nfsd`. Merge order: top-level `env_vars`, then framework defaults (`ENABLE_LOCKSTAT`, `GSS_USE_HOSTNAME`, `CEPH_CONF`), then this section (later wins). Values are double-quoted, allowing `$VAR`/`${VAR}` expansion. |

#### Profiling

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `perf_record` | bool | `false` | Enable `perf record` profiling on Ganesha |
| `perf_record_script` | string | | Path to perf recording script |
| `perf_record_executable` | string | `ganesha.nfsd` | Executable name to attach perf to |
| `perf_record_duration` | int | | Profiling duration in seconds |

#### Lock Statistics

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `lockstat.enabled` | bool | `false` | Collect lock contention statistics |
| `lockstat.path` | string | `/usr/local/bin/ceph-lockstat` | Path to `ceph-lockstat` binary |
| `lockstat.threshold` | int | `0` | Lock contention reporting threshold |

---

### `rgw`

Controls Ceph Object Gateway (RGW) deployment for S3 benchmarking. When enabled, the runner uses `StubMountManager` (no filesystem mounts) and provisions RGW on inventory hosts in the `rgws` group. An elbencho workload generator will be added in a follow-up.

#### Core

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Enable RGW deployment |
| `type` | string | from `fs_manager_type` | Deployment type (`cephadm` or `systemd`) |
| `service_id` | string | `rgw` | Cephadm service id (e.g. `rgw.eot`) |
| `host_label` | string | `rgw` | Cephadm orch host label applied to `rgws` hosts |
| `count_per_host` | int or list | `1` | Number of `radosgw` daemons per host (matrix-expandable) |
| `frontend_port` | int or list | `7480` | First beast port; instances use `port` .. `port+count_per_host-1` |
| `yaml_path` | string | `/cephfs_perf/rgw.yaml` | Remote path for the cephadm RGW spec |
| `ceph_binary_path` | string | `${CEPH_INSTALL_PREFIX}/bin/ceph` | Path to the `ceph` CLI |
| `radosgw_binary_path` | string | `${CEPH_INSTALL_PREFIX}/bin/radosgw` | Path to `radosgw` (systemd only) |
| `radosgw_admin_binary_path` | string | `${CEPH_INSTALL_PREFIX}/bin/radosgw-admin` | Path to `radosgw-admin` |
| `pid_dir` | string | `/var/run/ceph` | PID file directory (systemd only) |
| `manage_firewall` | bool | `true` | Open frontend ports via firewalld/iptables when those services are active |

#### Authentication / S3 user

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `user_id` | string | inherits `ceph.user_id` | Ceph client used for `ceph` / `radosgw-admin` |
| `keyring_path` | string | inherits `ceph.keyring` | Keyring for admin commands |
| `uid` | string | `perfuser` | S3 user uid created with `radosgw-admin user create` |
| `display_name` | string | `Perf User` | S3 user display name |
| `access_key` | string | | Optional fixed access key (auto-generated if omitted) |
| `secret_key` | string | | Optional fixed secret key (auto-generated if omitted) |
| `credentials_path` | string | `/cephfs_perf/rgw/s3_credentials.json` | Where access keys and endpoint URLs are written for workloads |

#### Example (cephadm)

Matches a typical orch spec with label placement and multiple daemons per host:

```yaml
rgw:
  enabled: true
  type: "cephadm"
  service_id: "eot"
  host_label: "rgw"
  count_per_host: 4
  frontend_port: 7480
  uid: "perfuser"
  display_name: "Perf User"
  access_key: "PERFACCESSKEY"
  secret_key: "PERFSECRETKEY0123456789"
  credentials_path: "/cephfs_perf/rgw/s3_credentials.json"
```

Generated cephadm spec (conceptually):

```yaml
service_type: rgw
service_id: eot
placement:
  label: rgw
  count_per_host: 4
spec:
  rgw_frontend_port: 7480
```

Enable with `--rgw cephadm` or `--rgw systemd`, and ensure inventory includes an `rgws` group.

---

### `elbencho`

Configuration for the elbencho S3 workload runner (`cephfs_elbencho_runner.py`). This runner forces `StubFSManager` + RGW provisioning, generates an `s3-classic.conf`-style file from RGW credentials/endpoints and inventory clients, then drives elbencho with `--jsonfile` output.

Each result JSON under `results_base_dir` includes `test_parameters` and `test_results_summary` (same pattern as rados/fio).

#### Global Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `results_base_dir` | string | `/cephfs_perf/results` | Base directory for result files |
| `run_command` | string | `/cephfs_perf/elbencho/run_elbencho_workload.py` | Remote driver script path |
| `executable_path` | string | `/usr/local/bin/elbencho` | Path to the `elbencho` binary (S3-enabled build) |
| `conf_path` | string | `/cephfs_perf/rgw/s3-classic.conf` | Where the generated bash-style conf is written |
| `size_limit` | string | `4T` | Written to conf as `SIZE_LIMIT` |
| `object_limit` | int | `1000000` | Written to conf as `OBJECT_LIMIT` |
| `load_driver_port` | int | `1611` | Elbencho service port (`LOAD_DRIVER_PORT` / `--port`) |
| `distributed` | bool | `true` | Start `elbencho --service` on clients and use `--hosts` |
| `manage_firewall` | bool | inherits `rgw.manage_firewall` | Open `load_driver_port` on clients (and admin) via firewalld/iptables |
| `buckets` | list | `["eot-classic"]` | S3 bucket names (`BUCKET_LIST`) |
| `env_vars` | dict | `{}` | Extra environment variables for elbencho |

S3 keys and `RGW_HOSTS` are taken from `rgw.credentials_path` (written when RGW is provisioned). `CLIENTS` are inventory `clients` private IPs.

#### Loadpoint Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `readwrite` | string or list | `write` | `write`, `read`, `writeread`, `delete`, `mkdirs` |
| `threads` | int or list | `16` | Threads per host (`--threads`) |
| `size` | string | `4MiB` | Object size (`--size`); use IEC (`KiB`/`MiB`/`GiB`) to match `numfmt --from=iec` |
| `block-size` | string | same as `size` | Block/part size (`--block`) |
| `files` | int | `1000` | Objects per directory (`--files` / `-N`) |
| `dirs` | int | `1` | Directories per thread (`--dirs` / `-n`) |
| `blockvarpct` | int | | PUT-only: passed as `--blockvarpct` (benchmark-s3.sh random data %) |
| `s3fastget` | bool | `false` | GET-only: pass `--s3fastget` |
| `timelimit` | int | | Optional `--timelimit` seconds |
| `extra_args` | string | | Extra elbencho CLI arguments |

#### Example

```yaml
elbencho:
  results_base_dir: "/cephfs_perf/results"
  executable_path: "/usr/local/bin/elbencho"
  conf_path: "/cephfs_perf/rgw/s3-classic.conf"
  buckets: ["eot-classic"]
  load_driver_port: 1611
  distributed: true
  loadpoints:
    - readwrite: ["write", "read"]
      threads: [32, 16]
      size: "4MiB"
      block-size: "4MiB"
      files: 1000
      dirs: 1
```

```bash
./cephfs_elbencho_runner.py MDSConfigurationSettings.yml --rgw cephadm
```

---

### `cephfs_tool`

Configuration for the `cephfs-tool bench` workload runner.

#### Global Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `results_base_dir` | string | `/cephfs_perf/results` | Base directory for result files |
| `run_command` | string | `/cephfs_perf/cephfs_tool/run_cephfs_workload.py` | Remote driver script path |
| `executable_path` | string | `/usr/local/bin/cephfs-tool` | Path to `cephfs-tool` binary |
| `config_path` | string | `/etc/ceph/ceph.conf` | ceph.conf path |
| `keyring` | string | | Keyring path |
| `client_id` | string | `admin` | Ceph client ID |
| `root_path` | string | `/` | Root path within the filesystem |
| `duration` | int | `0` | Global test duration limit in seconds (0 = unlimited) |
| `progress` | bool | `true` | Show progress output |
| `progress_interval` | int | `10` | Progress update interval in seconds |
| `msgr_workers` | int | | Default messenger worker count (overridable per loadpoint) |
| `env_vars` | dict | `{}` | Extra environment variables merged on top of top-level `env_vars` for `cephfs-tool`. Values are double-quoted, allowing `$VAR`/`${VAR}` expansion. Common use: `CEPH_ARGS`. |

#### Profiling

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `perf_record` | bool | `false` | Enable `perf record` profiling |
| `perf_record_script` | string | | Path to perf recording script |
| `perf_record_executable` | string | `cephfs-tool` | Executable to profile |
| `perf_record_duration` | int | `30` | Profiling duration in seconds |

#### Lock Statistics

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `lockstat.enabled` | bool | `false` | Collect lock contention statistics |
| `lockstat.path` | string | `/usr/local/bin/ceph-lockstat` | Path to `ceph-lockstat` |
| `lockstat.asok` | string | `/var/run/ceph/cephfs-tool.asok` | Admin socket path |
| `lockstat.threshold` | int | `0` | Reporting threshold |

#### Loadpoint Options

Each entry in `loadpoints` is a dict (or expanded from lists via Cartesian product):

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `files` | int | `1024` | Number of files per thread |
| `size` | string | `128MiB` | File size (SI units: `1GiB`, `256MiB`) |
| `iterations` | int | `3` | Number of write+read iterations |
| `threads` | int or list | `32` | Number of concurrent threads |
| `msgr_workers` | int or list | | Messenger workers (overrides global) |
| `block-size` | string or list | | I/O block size (e.g., `4MiB`, `256KiB`) |
| `client-oc` | int or list | | Client object cache (`0`=off, `1`=on) |
| `client-oc-size` | string or list | | Object cache size |
| `async` | bool | | Enable async I/O mode |
| `queue-depth` | int or list | | Queue depth for async mode |
| `extra_args` | string | | Additional arguments passed to `cephfs-tool bench` |

---

### `fio`

Configuration for the fio workload runner.

#### Global Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `results_base_dir` | string | `/cephfs_perf/results` | Base directory for result files |
| `run_command` | string | `/cephfs_perf/fio/run_fio_workload.py` | Remote driver script path |
| `mounts_per_fs` | int | `1` | Number of mount points per filesystem per client |
| `gtod_reduce` | int | `1` | Enable `gtod_reduce` to reduce gettimeofday overhead |
| `ramp_time` | int | `5` | Warmup time in seconds before measuring |
| `threads_fio` | bool | | Use threads instead of forked processes |

#### Profiling

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `perf_record` | bool | `false` | Enable `perf record` profiling |
| `perf_record_script` | string | | Path to perf recording script |
| `perf_record_executable` | string | `ganesha.nfsd` | Executable to profile |
| `perf_record_duration` | int | | Profiling duration in seconds |
| `flamegraph_path` | string | | Path to FlameGraph tools for SVG generation |
| `stap_script` | string | | Path to a SystemTap script to run alongside fio |

#### Loadpoint Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `duration` | int | | Test runtime in seconds |
| `size` | string | | Total I/O size per job (e.g., `5GiB`) |
| `block-size` | string or list | | I/O block size (maps to `--bs`) |
| `iodepth` | int or list | | I/O queue depth |
| `readwrite` | string or list | | Access pattern (`randread`, `randwrite`, `randrw`, `read`, `write`) |
| `rwmixread` | int or list | | Read percentage for `randrw` (e.g., `75`) |
| `ioengine` | string or list | | I/O engine (e.g., `libaio`) |
| `direct` | int or list | | Direct I/O (`1`=on, `0`=off) |
| `buffered` | int | | Buffered I/O mode |
| `create_serialize` | int | | Serialize file creation (`0` or `1`) |
| `threads` | int or list | | Number of fio jobs |
| `ramp_time` | int | | Per-loadpoint ramp time override |
| `extra_args` | string | | Additional fio command-line arguments |

---

### `rados_bench`

Configuration for the `rados bench` workload runner. This workload targets a RADOS pool directly (not CephFS), making it useful for isolating OSD-level performance from MDS overhead.

Set `mount_manager_type: StubMountManager` when running rados bench only — no filesystem mounts are needed.

#### Global Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `results_base_dir` | string | `/cephfs_perf/results` | Base directory for result files |
| `run_command` | string | `/cephfs_perf/rados_bench/run_rados_workload.py` | Remote driver script path |
| `executable_path` | string | `/usr/local/bin/rados` | Path to the `rados` binary |
| `config_path` | string | `/etc/ceph/ceph.conf` | Path to `ceph.conf` |
| `keyring` | string | | Path to Ceph keyring file |
| `client_id` | string | `admin` | Ceph client user ID |
| `pool` | string | required | RADOS pool to benchmark. Created automatically by `CephPoolManager` if it does not exist. |
| `pool_pg_num` | int | | PG count for the pool (optional) |
| `pool_size` | int | | Replication size (optional) |
| `pool_min_size` | int | | Minimum replication size (optional) |
| `pool_recreate` | bool | `false` | Wipe and recreate the pool before each iteration |
| `no_cleanup` | bool | `true` | Keep bench objects after write phase so subsequent read loadpoints can find them |
| `duration` | int | `30` | Default bench duration in seconds (overridable per loadpoint) |
| `env_vars` | dict | `{}` | Extra environment variables merged on top of top-level `env_vars` for `rados`. Values are double-quoted, allowing `$VAR`/`${VAR}` expansion. |

#### Profiling

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `perf_record` | bool | `false` | Enable `perf record` profiling |
| `perf_record_script` | string | | Path to perf recording script |
| `perf_record_executable` | string | `rados` | Executable to profile |
| `perf_record_duration` | int | `30` | Profiling duration in seconds |

#### Loadpoint Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `readwrite` | string or list | `seqwrite` | Access pattern: `seqwrite`, `randwrite`, `seqread`, `randread` |
| `threads` | int or list | `16` | Number of concurrent I/O streams (`--concurrent-ios`) |
| `duration` | int | global `duration` | Per-loadpoint bench duration in seconds |
| `min-object-size` | string | | Minimum object size for write loadpoints (e.g., `4KiB`). SI units supported. |
| `max-object-size` | string | | Maximum object size for write loadpoints (e.g., `4MiB`). SI units supported. |
| `read-percent` | int | | Read percentage (passed as `--read-percent`; for mixed workloads) |
| `no_cleanup` | bool | global `no_cleanup` | Per-loadpoint override for `--no-cleanup` |
| `run_name` | string | `<fs_name>_<client>` | Override the `--run-name` prefix. Useful when chaining write and read loadpoints across separate runs. |
| `extra_args` | string | | Additional arguments appended verbatim to the `rados bench` command |

> **Note on read loadpoints**: `seqread` and `randread` require objects from a prior `seqwrite`/`randwrite` run with the same `--run-name`. Set `no_cleanup: true` on the write loadpoint so objects persist for subsequent reads.

#### Example Configuration

```yaml
mount_manager_type: "StubMountManager"

rados_bench:
  results_base_dir: "/cephfs_perf/results"
  run_command: "/cephfs_perf/rados_bench/run_rados_workload.py"
  executable_path: "/usr/local/bin/rados"
  config_path: "/etc/ceph/ceph.conf"
  keyring: "/etc/ceph/ceph.client.admin.keyring"
  client_id: "admin"
  pool: "rados_bench_pool"
  no_cleanup: true
  duration: 30
  loadpoints:
    - readwrite: ["seqwrite", "randwrite", "seqread", "randread"]
      threads: [16, 32]
      duration: 30
      min-object-size: "4KiB"
      max-object-size: "4MiB"
      extra_args: ""
```

---

### `rbd`

Configuration for the RBD workload runner, which uses fio's `rbd` ioengine (librbd) to benchmark a RADOS block device pool directly. Like `rados_bench`, this targets the OSD layer without CephFS/MDS involvement.

Set `mount_manager_type: StubMountManager` — no filesystem mounts are needed.

RBD images are created once per client before any loadpoints run and reused across all loadpoints unless `recreate_images: true`.

#### Global Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `results_base_dir` | string | `/cephfs_perf/results` | Base directory for result files |
| `run_command` | string | `/cephfs_perf/rbd/run_rbd_workload.py` | Remote driver script path |
| `executable_path` | string | `/usr/local/bin/fio` | Path to the `fio` binary |
| `rbd_executable_path` | string | `/usr/local/bin/rbd` | Path to the `rbd` binary (used for image management) |
| `config_path` | string | | Path to `ceph.conf` |
| `keyring` | string | | Path to Ceph keyring file |
| `client_id` | string | `admin` | Ceph client user ID (passed as `--clientname`) |
| `pool` | string | required | RBD pool name. Created and initialized by `CephPoolManager` if it does not exist. |
| `pool_pg_num` | int | | PG count for the pool (optional) |
| `pool_size` | int | | Replication size (optional) |
| `pool_min_size` | int | | Minimum replication size (optional) |
| `pool_recreate` | bool | `false` | Wipe and recreate the pool before each iteration |
| `image_size` | string | `10GiB` | Size of each RBD image (SI units supported) |
| `images_per_client` | int | `1` | Number of RBD images created per client. fio runs one job per image. |
| `recreate_images` | bool | `false` | Delete and recreate images before each iteration |
| `gtod_reduce` | int | `1` | Enable fio `gtod_reduce` to reduce `gettimeofday` overhead |
| `ramp_time` | int | `5` | Warmup time in seconds before measuring |
| `randrepeat` | int | | fio `randrepeat` setting |
| `timestamp_progress` | bool | `false` | Prefix each progress line with an ISO 8601 UTC timestamp |
| `env_vars` | dict | `{}` | Extra environment variables merged on top of top-level `env_vars` for `fio`. Values are double-quoted, allowing `$VAR`/`${VAR}` expansion. |

#### Profiling

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `perf_record` | bool | `false` | Enable `perf record` profiling |
| `perf_record_script` | string | | Path to perf recording script |
| `perf_record_executable` | string | `fio` | Executable to profile |
| `perf_record_duration` | int | `30` | Profiling duration in seconds |
| `flamegraph_path` | string | | Path to FlameGraph tools for SVG generation |

#### Loadpoint Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `readwrite` | string or list | | Access pattern (`randread`, `randwrite`, `randrw`, `read`, `write`) |
| `block-size` | string or list | | I/O block size (e.g., `4MiB`, `256KiB`) |
| `iodepth` | int or list | | I/O queue depth |
| `direct` | int or list | | Direct I/O (`1`=on, `0`=off) |
| `rwmixread` | int or list | | Read percentage for `randrw` mode |
| `threads` | int or list | | Number of fio jobs (maps to `--numjobs`) |
| `size` | string | | Total I/O size per job |
| `duration` | int | global `duration` | Per-loadpoint runtime in seconds |
| `gtod_reduce` | int | global value | Per-loadpoint override |
| `ramp_time` | int | global value | Per-loadpoint ramp time override |
| `randrepeat` | int | global value | Per-loadpoint override |
| `extra_args` | string | | Additional fio arguments appended verbatim |

> **Note**: Do not set `create_serialize: 0` for RBD loadpoints. fio's `rbd` ioengine can race on concurrent connect when `numjobs > 1` and setup runs inside each thread. Leave it at fio's default (`1`).

#### Example Configuration

```yaml
mount_manager_type: "StubMountManager"

rbd:
  results_base_dir: "/cephfs_perf/results"
  run_command: "/cephfs_perf/rbd/run_rbd_workload.py"
  executable_path: "/usr/local/bin/fio"
  rbd_executable_path: "/usr/local/bin/rbd"
  config_path: "/etc/ceph/ceph.conf"
  keyring: "/etc/ceph/ceph.client.admin.keyring"
  client_id: "admin"
  pool: "rbd_bench_pool"
  image_size: "10GiB"
  images_per_client: 1
  recreate_images: false
  gtod_reduce: 1
  ramp_time: 5
  loadpoints:
      duration: 60
      block-size: ["4MiB", "256KiB"]
      iodepth: [8]
      readwrite: ["randwrite", "randread"]
      direct: [1]
      threads: [32, 8]
      extra_args: ""
```

---

### `specstorage`

Configuration for the SPECstorage 2020 workload runner.

#### Global Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `results_base_dir` | string | `/cephfs_perf/results` | Base directory for result files |
| `run_command` | string | `/cephfs_perf/sfs2020/run_sfs2020_workload.py` | Remote driver script path |
| `output_path` | string | `/cephfs_perf/sfs2020/spec_2020.txt` | Path where the generated spec file is written |
| `workload_dir` | string | `/cephfs_perf/sfs2020/SPECstorage2020` | SPECstorage installation directory on admin host |
| `netmist_env` | string | `netmist.env` | Path to a local YAML file with license key and paths |
| `benchmark` | string | `SWBUILD` | Workload type (`SWBUILD`, `VDA`, `EDA_BLENDED`, `AI_IMAGE`, `ENOMICS`) |
| `mounts_per_fs` | int | `1` | Number of mount points per filesystem per client |
| `increment` | int | `1` | Increment between load points (maps to `INCR_LOAD`) |
| `num_runs` | int | `1` | Number of runs per load point (maps to `NUM_RUNS`) |

#### Profiling

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `perf_record` | bool | `false` | Enable `perf record` profiling |
| `perf_record_script` | string | | Path to perf recording script |
| `perf_record_executable` | string | `ceph-mds` | Executable to profile |
| `perf_record_duration` | int | | Profiling duration in seconds |

#### Lock Statistics

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `lockstat.enabled` | bool | `false` | Collect lock contention statistics |
| `lockstat.path` | string | `/usr/local/bin/ceph-lockstat` | Path to `ceph-lockstat` |
| `lockstat.threshold` | int | `0` | Reporting threshold |

#### Loadpoint Options

| Key | Type | Description |
|-----|------|-------------|
| `loadpoints` | list of int | Load metric values to test (e.g., `[1, 2, 4, 8]`) |

#### `netmist_env` File

A local YAML file providing SPECstorage licensing information:

```yaml
netmist_license_key: 1234
netmist_license_path: "/tmp/netmist_license_key"
sfs2020_archive: "/path/to/SPECstorage2020.tgz"  # optional
```

---

### `logging`

Controls MDS debug logging during tests. Prefer nesting under ``mds.logging``;
a top-level ``logging`` section is still accepted for older settings files.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Enable MDS debug logging |
| `debug_mds` | int or string | `5` | MDS subsystem debug level. Use ``<log>/<memory>`` (e.g. ``1/20``) to keep a verbose gather level in memory while writing only the lower log level to disk |
| `debug_ms` | int or string | `1` | Messenger subsystem debug level (same ``log/memory`` form supported) |
| `memory` | bool | auto | Flush the in-memory recent log buffer via admin-socket ``log dump`` before collecting logs. Defaults to on when ``debug_mds`` uses ``log/memory`` form or ``log_max_recent`` is set |
| `log_max_recent` | int | | Size of the in-memory recent-log ring buffer (Ceph daemon default is ``10000``). Applied via ``ceph config set mds log_max_recent`` |

```yaml
mds:
  logging:
    enabled: true
    debug_mds: 1/20
    debug_ms: 1
    memory: true
    log_max_recent: 10000
```

---

### `mds_settings`

MDS parameters swept across the test matrix. Each key maps to a single value or list of values; lists are expanded into the Cartesian product with other list settings.

| Key | Type | Example | Description |
|-----|------|---------|-------------|
| `mds_cache_memory_limit` | string or list | `[128Gi]` | MDS cache memory limit |
| `max_mds` | int or list | `[1, 2]` | Maximum number of active MDSs |
| `cpus` | int or list | `[4, 8]` | CPU cores allocated to MDS |
| `mds_max_caps_per_client` | string or list | `[100k, 200k]` | Max capabilities per client |
| `mds_recall_max_caps` | string or list | `[5k, 10k]` | Max caps to recall at once |
| `mds_recall_max_decay_rate` | int or list | `[1, 2]` | Decay rate for cap recall |
| `mds_cache_trim_threshold` | string or list | `[64Ki, 128Ki]` | Cache trim threshold |
| `mds_cache_reservation` | int or list | `[5, 10]` | Cache reservation percentage |
| `mds_log_max_segments` | int or list | `[30, 60]` | Maximum MDS log segments |
| `mds_dispatch_engine` | string or list | `[classic, reactor]` | MDS dispatch engine (`classic` or `reactor`). Applied before MDS daemons start via a separate MDS config file (default `/etc/ceph/mds-settings.conf`) that merges the cluster `ceph.conf` with MDS-only overrides, so mons are not affected. Set `mds.dispatch_engine_via: ceph-config` when mons support the option. |

```yaml
mds_settings:
  mds_dispatch_engine: [classic, reactor]
```

For vstart / clusters where monitors run the same wip build as clients:

```yaml
mds:
  dispatch_engine_via: ceph-config

mds_settings:
  mds_dispatch_engine: [classic, reactor]
```

---

## Mount Managers

Selected via `mount_manager_type`.

### `MountKernelManager`

Mounts CephFS directly via the kernel client.

- No additional configuration keys.
- Uses `fs_name`, the cluster's monitor addresses, and the Ceph keyring.

### `MountFuseManager`

Mounts CephFS via `ceph-fuse` on each client.

- Selected with `mount_manager_type: MountFuseManager` or `--mount-manager MountFuseManager`.
- Monitor addresses are resolved on the admin host; auth defaults to the top-level `ceph:` section.

Configured via the `mount_fuse` section:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `binary_path` | string | `${CEPH_INSTALL_PREFIX}/bin/ceph-fuse` | Path to the `ceph-fuse` executable |
| `client_id` | string | `ceph.user_id` | Ceph client id passed to `--id` |
| `keyring` | string | `ceph.keyring` | Keyring path passed to `-k` |
| `conf` | string | `ceph.conf` | Ceph config path passed to `-c` |
| `mount_options` | string | *(empty)* | Extra arguments appended to the `ceph-fuse` command |
| `env_vars` | dict | *(empty)* | Extra env vars merged on top of top-level `env_vars` |

```yaml
mount_manager_type: "MountFuseManager"
mount_fuse:
  binary_path: "${CEPH_INSTALL_PREFIX}/bin/ceph-fuse"
  client_id: "admin"
  keyring: "/etc/ceph/ceph.client.admin.keyring"
  conf: "/etc/ceph/ceph.conf"
```

### `MountNfsManager`

Mounts via NFS through Ganesha.

- Requires `ganesha.enabled: true`.
- Distributes clients across Ganesha nodes in round-robin order.
- `mounts_per_fs` controls the number of mount points per client per filesystem.

Configured via the `mount_nfs` section:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `mount_options` | string | `nfsvers=4.1,proto=tcp` | Options passed to `mount -t nfs -o` |

```yaml
mount_nfs:
  mount_options: "nfsvers=4.1,proto=tcp,sec=sys"
```

### `StubMountManager`

No-op mount manager. Used for workloads that manage their own connectivity (e.g., `cephfs_tool` uses its own libcephfs handle).

---

## Inventory Providers

### `DirectInventoryProvider`

Parses a YAML-based inventory defined directly in the config file under `inventory`. Used automatically when no external Ansible inventory file is provided.

> **Note**: A `mons` group is required. The first host in `mons` is designated the **admin host**.

Supported groups: `mons`, `mgrs`, `clients`, `ganeshas`, `sambas`, `rgws`, `grafanas`, `mdss`, `osds`.

Per-host fields:

| Field | Required | Description |
|-------|----------|-------------|
| `ansible_ssh_host` | yes | IP or hostname for SSH |
| `ansible_ssh_user` | yes | SSH username |
| `ansible_ssh_port` | no | SSH port (default: `22`) |
| `ansible_ssh_private_key_file` | no | Path to SSH private key |
| `private_ip` | no | Cluster-internal IP for client-to-cluster traffic |

```yaml
inventory:
  mons:
    mon-000:
      ansible_ssh_host: 169.63.188.95
      ansible_ssh_user: root
      private_ip: 10.241.64.69
  clients:
    client-000:
      ansible_ssh_host: 169.63.179.214
      ansible_ssh_user: root
      private_ip: 10.241.64.70
  grafanas:
    mon-000:
      ansible_ssh_host: 169.63.188.95
      ansible_ssh_user: root
      private_ip: 10.241.64.69
```

The `grafanas` group identifies hosts for Grafana and Prometheus placement. When `grafana.enabled` is true and no `grafanas` group is defined, the first `mons` host is used.

### `AnsibleInventoryProvider`

Parses an Ansible-style INI inventory file.

> **Note**: A `[mons]` group is required.

- Loads global variables from `group_vars/all.yml` and `cluster.json` relative to the project parent directory.
- Supports `{{ var_name }}` template syntax.

```ini
[mons]
mon-000 ansible_ssh_user=root ansible_ssh_host=10.241.64.69

[clients]
client-000 ansible_ssh_user=root ansible_ssh_host=10.241.64.70
```

---

## Performance Recording

| Option | Description |
|--------|-------------|
| `perf_record: true` | Captures `perf.data`, generates text reports and SVG flamegraphs via FlameGraph |
| `perf_record_executable` | Name of the process to attach `perf record` to |
| `perf_record_duration` | How long to record in seconds |
| `flamegraph_path` | Path to FlameGraph tool directory (fio only) |
| `stap_script` | Path to a SystemTap `.stp` script to run alongside the workload (fio only) |

---

## Result Naming Convention

Output files follow the pattern:

```
<workload>_<output_type>_<client>_lp<N>_<encoded_settings>.json
```

- `workload`: `cephfs_tool`, `fio`, or `sfs2020`
- `output_type`: `result`, `perf_dump`
- `N`: load point number
- `encoded_settings`: abbreviated key-value pairs for the active parameters (e.g., `s5GiB_t32_oc1_ocs16GiB_bs4MiB_mw8`)

Results are stored in a timestamped directory under `results_base_dir`.
