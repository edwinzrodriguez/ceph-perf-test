import abc
import re

from cephfs_perf_lib import CommonUtils, FSManager


class SambaManager(abc.ABC):
    def __init__(self, executor, config, fs_manager):
        self.executor = executor
        self.config = config
        self.fs_manager = fs_manager
        self.sambas = config.sambas
        self.admin = config.admin_host
        self._provisioned = False

    @abc.abstractmethod
    def provision_samba(self, results_dir=None):
        pass

    @abc.abstractmethod
    def cleanup_samba(self):
        pass

    @staticmethod
    def get_samba_ceph_vfs_config_str(settings):
        return CommonUtils.get_samba_ceph_vfs_config_str(settings)

    @staticmethod
    def get_samba_ceph_vfs_path_parts(config=None, settings=None):
        return CommonUtils.get_samba_ceph_vfs_path_parts(
            config=config, settings=settings
        )

    @staticmethod
    def get_samba_config_str(settings):
        parts = []
        if "clustering" in settings:
            parts.append(
                f"{CommonUtils.get_short_name('Samba Clustering')}{settings['clustering']}"
            )
        if "workgroup" in settings:
            parts.append(
                f"{CommonUtils.get_short_name('Samba Workgroup')}{settings['workgroup']}"
            )
        vfs_part = CommonUtils.get_samba_ceph_vfs_config_str(settings)
        if vfs_part:
            parts.append(vfs_part)
        return "_".join(parts)

    @staticmethod
    def samba_payload_keys():
        """Config property names copied into workload settings payloads."""
        return [
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

    def safe_json_load(self, raw, default=None):
        return FSManager.safe_json_load(self, raw, default)

    def get_fs_names(self):
        return self.fs_manager.get_fs_names()

    def _get_ceph_args(self, include_keyring=True):
        args = []
        if self.config.ceph_conf_path:
            args.append(f"-c {self.config.ceph_conf_path}")
        user_id = self.config.samba_user_id
        if user_id:
            args.append(f"--user {user_id}")
        if include_keyring and self.config.samba_keyring_path:
            args.append(f"--keyring {self.config.samba_keyring_path}")
        return " ".join(args)

    def _get_samba_env(self):
        default_env = {
            "CEPH_CONF": self.config.ceph_conf_path,
        }
        return self.config.get_merged_env_vars(
            default_env, self.config.samba_env_vars
        )

    def _sudo_with_samba_env(self, cmd):
        return CommonUtils.with_env_exports(cmd, self._get_samba_env(), sudo=True)

    def _ceph_bin(self):
        return self.config.samba_ceph_binary_path

    def _run_ceph(self, args, check=False):
        ceph_bin = self._ceph_bin()
        return self.executor.run_remote(
            self.admin,
            self._sudo_with_samba_env(f"{ceph_bin} {self._get_ceph_args()} {args}"),
            check=check,
        )

    def share_name_for_fs(self, fs):
        prefix = self.config.samba_share_prefix
        return f"{prefix}{fs}" if prefix else fs

    def client_share_name_for_fs(self, fs):
        """Share name presented to SMB/CIFS clients (underscores allowed)."""
        return self.share_name_for_fs(fs)

    def share_id_for_fs(self, fs):
        """SMB share_id for ceph.smb.share (DNS fragment: no underscores).

        Ceph validates share_id with ``^[a-zA-Z0-9]($|[a-zA-Z0-9-]{,16}[a-zA-Z0-9]$)``,
        so filesystem names like ``perf_test_mds`` must be sanitized.
        """
        raw = self.client_share_name_for_fs(fs)
        sid = re.sub(r"[^a-zA-Z0-9-]", "-", raw)
        sid = re.sub(r"-+", "-", sid).strip("-")
        if not sid:
            sid = "share"
        if not sid[0].isalnum():
            sid = "s" + sid
        # Max length 18: leading alnum + up to 16 interior + trailing alnum.
        if len(sid) > 18:
            sid = sid[:18].rstrip("-")
        if len(sid) > 1 and not sid[-1].isalnum():
            sid = sid.rstrip("-")
        if not sid:
            sid = "share"
        return sid

    def subvolume_name_for_fs(self, fs):
        """CephFS subvolume name backing an SMB share (valid ID, no underscores)."""
        return self.share_id_for_fs(fs)

    def _ensure_smb_subvolumes(self):
        """Create mode-0777 subvolumes used as writable SMB share backends."""
        group = self.config.samba_subvolume_group
        mode = self.config.samba_subvolume_mode
        for fs in self.get_fs_names():
            subvol = self.subvolume_name_for_fs(fs)
            self.fs_manager._run_ceph(
                self.admin, f"fs subvolumegroup create {fs} {group} || true"
            )
            self.fs_manager._run_ceph(
                self.admin,
                f"fs subvolume create {fs} {subvol} "
                f"--group-name={group} --mode={mode} || true",
            )

    def _subvolume_backend_path(self, fs):
        """Absolute path within a mounted CephFS volume for a share subvolume."""
        group = self.config.samba_subvolume_group
        subvol = self.subvolume_name_for_fs(fs)
        subvol_path = self.fs_manager._run_ceph(
            self.admin,
            f"fs subvolume getpath {fs} {subvol} --group_name {group}",
        ).strip()
        if not subvol_path.startswith("/"):
            subvol_path = f"/{subvol_path}"
        return subvol_path
