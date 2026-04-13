from lib.ganesha.ganesha_manager import GaneshaManager


class GaneshaSystemdManager(GaneshaManager):
    def provision_ganesha(self, use_custom=True, results_dir=None):
        # To be implemented: provision ganesha directly via systemd
        pass

    def cleanup_ganesha(self):
        # To be implemented: cleanup ganesha provisioned via systemd
        pass
