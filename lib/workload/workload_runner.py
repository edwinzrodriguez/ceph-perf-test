import abc


class WorkloadRunner(abc.ABC):
    def __init__(self, executor, config, fs_names):
        self.executor, self.config, self.fs_names = executor, config, fs_names
        self.admin = config.admin_host

    @abc.abstractmethod
    def run_workload(
            self,
            settings,
            shared_ts=None,
            cephfs_manager=None,
            ganesha_manager=None,
    ):
        pass

    @abc.abstractmethod
    def get_results_dir(self, settings, shared_ts=None):
        pass

    @abc.abstractmethod
    def prepare_storage(self):
        pass
