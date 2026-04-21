# CephFS Performance Test Framework

A structured framework for running performance benchmarks on CephFS using various workload drivers, supporting both kernel mounts and NFS-Ganesha.

## Theory of Operation

The framework operates on a **Test Matrix** principle. It iterates through combinations of MDS settings and Ganesha settings, automatically provisioning the environment for each iteration.

1.  **Configuration**: Defined in a YAML file (e.g., `MDSConfigurationSettings.yml`).
2.  **Matrix Expansion**: If any setting in `mds_settings` or `ganesha` is a list, the framework calculates the Cartesian product of all combinations.
3.  **Iteration Lifecycle**:
    -   Unmount clients.
    -   Rebuild/Reset CephFS (optional, based on settings).
    -   Apply MDS configurations.
    -   Provision Ganesha (if enabled).
    -   Mount clients (Kernel or NFS).
    -   Expand and run Workload Loadpoints.
    -   Collect results and performance traces.

## Usage

```bash
./cephfs_perf_runner.py <config.yaml> <ansible_inventory>
```

## Workload Runners

The framework supports three main workload runners. Each supports **Loadpoint Expansion**: if a parameter (like `threads`) is a list, it will run separate benchmarks for each value.

### 1. CephFS-Tool (`cephfs_tool`)
Uses the internal `cephfs-tool bench` for low-level metadata and I/O testing.

**Loadpoint Options:**
-   `files`: Number of files per thread.
-   `size`: File size (supports SI units: `128MiB`, `1GiB`).
-   `threads`: Number of concurrent threads.
-   `iterations`: Number of times to run the loadpoint.
-   `block-size`: I/O block size (e.g., `4MB`, `1MiB`).
-   `client-oc`: Enable/disable object cache (`0` or `1`).
-   `client-oc-size`: Size of object cache (supports SI units).
-   `duration`: Limit each phase to N seconds.

### 2. Fio (`fio`)
Flexible I/O tester for industry-standard benchmarking.

**Loadpoint Options:**
-   `size`: Total I/O size.
-   `block-size`: I/O block size (maps to `--bs`).
-   `threads`: Number of jobs (maps to `--numjobs`).
-   `iodepth`: Number of I/O units to keep in flight.
-   `readwrite`: I/O pattern (`read`, `write`, `randread`, `randrw`, etc.).
-   `ioengine`: I/O engine to use (e.g., `libaio`).
-   `direct`: Use non-buffered I/O (`0` or `1`).
-   `buffered`: Use buffered I/O (`0` or `1`).
-   `duration`: Runtime in seconds.
-   `ramp_time`: Warmup time.
-   `gtod_reduce`: Enable `gtod_reduce` for reduced overhead.

### 3. SPECstorage 2020 (`spec_storage`)
Standardized benchmark for storage solution performance.

**Global Options:**
-   `workload_dir`: Path to the SPECstorage installation directory on the admin host.
-   `run_command`: Path to the remote driver script (default: `/cephfs_perf/sfs2020/run_sfs2020_workload.py`).
-   `output_path`: Path where the generated spec file will be saved on the admin host.
-   `netmist_env`: Path to a local file containing license keys and paths (default: `netmist.env`).
-   `benchmark`: The SPECstorage workload to run (e.g., `SWBUILD`, `VDA`, `VIDEO`).
-   `increment`: Increment value for loadpoints (maps to `INCR_LOAD`).
-   `num_runs`: Number of runs per loadpoint (maps to `NUM_RUNS`).
-   `mounts_per_fs`: Number of mount points per client per filesystem.

**Loadpoint Options:**
-   `loadpoints`: An array of numeric values representing the business metrics to test (e.g., `[1, 2, 4, 8]`).

#### `netmist_env` File
The SPECstorage runner requires a local environment file (specified by `netmist_env`) to provide licensing information. This file should be in YAML format with the following keys:
-   `netmist_license_key`: Your SPECstorage license key.
-   `netmist_license_path`: Remote path on the admin host where the license key file will be stored.
-   `sfs2020_archive`: (Optional) Path to the SPECstorage installation archive.

Example `netmist.env`:
```yaml
netmist_license_key: 1234
netmist_license_path: "/tmp/netmist_license_key"
sfs2020_archive: "/path/to/SPECstorage2020.tgz"
```

## Ganesha Options

When `ganesha_enabled` is true, the framework provisions NFS-Ganesha on specified hosts.

**Global Options:**
-   `type`: `systemd` or `cephadm`.
-   `worker_threads`: Number of Ganesha worker threads.
-   `ceph_binary_path`: Path to `ceph` binary (default: `/usr/bin/ceph`).
-   `user_id`: Ceph user ID for Ganesha (default: `admin`).
-   `keyring_path`: Path to the keyring for the specified user.

**FSAL_CEPH Specific Options (can be lists for matrix testing):**
-   `umask`: File mode creation mask.
-   `client_oc`: Enable/disable Ceph object cache.
-   `client_oc_size`: Size of the object cache.
-   `async`: Enable Ceph async operations.
-   `zerocopy`: Enable Ceph zero-copy I/O.

## Inventory Providers

The framework uses **Inventory Providers** to manage host information and SSH connectivity details.

### `AnsibleInventoryProvider`

Parses an Ansible-style INI inventory file.

> **Note**: A `[mons]` group is required, as the first host in this group is designated as the **admin host** to drive the performance tests.

- **Global Variables**: Loads from `group_vars/all.yml` and `cluster.json` (relative to the project parent directory).
- **Variable Expansion**: Supports `{{ var_name }}` syntax in inventory files.
- **Data Format**:
    - **Group Sections**: Expects groups like `[mons]`, `[clients]`, `[mdss]`, `[ganeshas]`.
    - **Host Metadata**: Each host entry can have attributes like `ansible_ssh_user`, `ansible_ssh_host`, and `ansible_ssh_port`.
    - **Example**:
      ```ini
      [clients]
      client-000 ansible_ssh_user=root ansible_ssh_host=10.241.64.100
      ```

### `DirectInventoryProvider`

Parses a YAML-based inventory structure, typically defined directly within the main configuration file under the `inventory` key. This provider is automatically used if no external Ansible inventory file is provided to `cephfs_perf_runner.py`.

> **Note**: A `mons` group is required, as the first host in this group is designated as the **admin host** to drive the performance tests.

- **Data Format**: A nested dictionary mapping group names to host names, where each host contains its metadata.
- **Required Host Fields**:
    - `ansible_ssh_host`: The IP address or hostname to connect to via SSH.
    - `ansible_ssh_user`: The username for SSH authentication.
    - `ansible_ssh_port`: (Optional) The SSH port (defaults to 22).
    - `ansible_ssh_private_key_file`: (Optional) Path to the private key for authentication.
    - `private_ip`: (Optional) The IP address on the cluster/private network, used for internal communication between clients and the cluster.
- **Example (YAML)**:
  ```yaml
  inventory:
    clients:
      client-000:
        ansible_ssh_host: 169.63.179.214
        ansible_ssh_user: root
        ansible_ssh_port: 22
        private_ip: 10.241.64.70
    mons:
      mon-000:
        ansible_ssh_host: 169.63.188.95
        ansible_ssh_user: root
        private_ip: 10.241.64.69
  ```

### `InventoryProvider` Interface

If you need a custom provider, implement the following abstract methods:
- `get_hosts()`: Returns a dict of group names to host metadata lists.
- `get_vars()`: Returns a dict of global variables.
- `get_all_hosts_meta()`: Returns a flat map of host names to metadata.

## Performance Recording

The framework can automatically capture performance data during benchmarks:
-   **Perf Record**: Captures `perf.data`, generates reports, and SVG flamegraphs.
-   **SystemTap**: Can execute custom `.stp` scripts (e.g., `stap_script` option).
-   **Perf Dump**: Collects internal Ceph performance counters.

## Result Naming Convention

Output files are standardized for easy analysis:
`<workload>_<output_type>_<client>_<lp>_<options>`

-   `workload`: `cephfs_tool`, `fio`, or `sfs2020`.
-   `output_type`: `result`, `perf_dump`, `perf_record`.
-   `options`: Encodes the specific settings used (e.g., `s128MiB_t32_oc1`).

Results are stored in a timestamped directory under `results/`.
