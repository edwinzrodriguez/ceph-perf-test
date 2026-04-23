import abc
import os


class MountManager(abc.ABC):
    def __init__(self, executor, config, fs_manager):
        self.executor = executor
        self.config = config
        self.clients = config.clients
        self.fs_names = fs_manager.get_fs_names()

    def unmount_clients(self):
        for c in self.clients:
            mnts = (
                self.executor.run_remote(
                    c, "awk '$2 ~ \"^/mnt/cephfs_\" {print $2}' /proc/mounts | sort -r"
                )
                .strip()
                .split()
            )
            for m in mnts:
                self.executor.run_remote(
                    c, f"sudo umount -f {m} || sudo umount -l {m} || true"
                )
            self.executor.run_remote(c, "sudo rm -rf /mnt/cephfs_*")

    @abc.abstractmethod
    def mount(self):
        pass


class StubMountManager(MountManager):
    def mount(self):
        pass

    def unmount_clients(self):
        pass
