from ceph.rbd.rbd import Rbd


class RbdMirror(Rbd):
    def __init__(self, nodes):
        self.nodes = nodes
        super(RbdMirror, self).__init__(nodes=nodes)

    def enable_pool_image():
        pass

    def disable_pool_image():
        pass

    def enable_image_journaling_feature():
        pass

    def mirror_snapshots():
        pass

    def promotion():
        pass

    def status():
        pass

    def demotion():
        pass

    def bootstrap_peers(self, pool, cluster):
        self.create_bootstrap_peers()
        self.import_bootstrap_peers()

    def remove_peers():
        pass

    def create_image_mirror_snapshots():
        pass

    def remove_image_mirror_snapshots():
        pass

    def status_image_mirror_snapshots():
        pass
