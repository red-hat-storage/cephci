from copy import deepcopy

from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class Group(Cli):
    def __init__(self, nodes, base_cmd):
        super(Group, self).__init__(nodes)
        self.base_cmd = base_cmd + " group"
        self.image = self.Image(parent=self, base_cmd=self.base_cmd)
        self.snap = self.Snap(parent=self, base_cmd=self.base_cmd)

    def create(self, **kw):
        """
        Creates a group in a given pool/[namespace], if pool is not given then create in default pool i.e rbd.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool(str) : name of the pool into which namespace should be stored,default pool is rbd
                namespace(str): namespace in the pool
                group(str): group name to be created
                group-spec(str): [<pool-name>/[<namespace>/]]<group-name>)
                See rbd help group create for more supported keys
        """
        kw_copy = deepcopy(kw)
        group_spec = kw_copy.pop("group-spec", "")
        cmd = f"{self.base_cmd} create {group_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def list(self, **kw):
        """
        lists all groups in a given pool/[namespace], if pool is not given then list in default pool i.e rbd.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool(str) : name of the pool into which namespace should be stored
                namespace(str): namespace in the pool
                pool-spec(str): <pool-name>[/<namespace>]
                format(str): json format output of listing namespace
                See rbd help group ls/list for more supported keys
        """
        kw_copy = deepcopy(kw)
        pool_spec = kw_copy.pop("pool-spec", "")
        cmd = f"{self.base_cmd} list {pool_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def info(self, **kw):
        """
        lists info of group snapshot, if pool is not given then list in default pool i.e rbd.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool(str) : name of the pool into which namespace should be stored
                namespace(str): namespace in the pool
                pool-spec(str): <pool-name>[/<namespace>]
                format(str): json format output of listing namespace
                See rbd help group ls/list for more supported keys
        """
        cmd = f"{self.base_cmd} info {build_cmd_from_args(**kw)}"

        return self.execute_as_sudo(cmd=cmd)

    def remove(self, **kw):
        """
        Removes a group in a given pool/[namespace], if pool is not given then remove in default pool i.e rbd.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool(str) : name of the pool into which namespace should be deleted
                namespace(str): namespace in the pool
                group(str): group to be removed
                group-spec(str): [<pool-name>/[<namespace>/]]<group-name>
                See rbd help group rm for more supported keys
        """
        kw_copy = deepcopy(kw)
        group_spec = kw_copy.pop("group-spec", "")
        cmd = f"{self.base_cmd} rm {group_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def rename(self, **kw):
        """
        Renames the group with in a pool or different pool [pool]/[namespace]<group>
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool(str) : name of the pool into which namespace should be stored,default pool is rbd
                namespace(str): namespace in the pool
                group(str): group name to be created
                dest-pool(str) : name of the destination pool into which group to be renamed
                dest-namespace(str): destination namespace in the pool
                dest-group(str): destination group
                source-group-spec(str): <pool-name>/[<namespace>/]]<group-name>
                dest-group-spec(str): <pool-name>/[<namespace>/]]<group-name>
        """
        kw_copy = deepcopy(kw)
        source_group_spec = kw_copy.pop("source-group-spec", "")
        destination_group_spec = kw_copy.pop("dest-group-spec", "")
        cmd = f"{self.base_cmd} rename {source_group_spec} {destination_group_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    class Image(object):
        def __init__(self, parent, base_cmd):
            self.base_cmd = base_cmd + " image"
            self.parent = parent

        def add(self, **kw):
            """
            Add image to the give group in the pool/[namespace]
            Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    group-pool(str) : name of the pool into which namespace should be deleted
                    group-namespace(str): namespace in the pool
                    group(str): group where image to be added
                    image-pool(str): image pool name
                    image-namespace(str): image namespace name
                    image(str): image name
                    pool(str): pool name unless overridden
                    group-spec(str): [<pool-name>/[<namespace>/]]<group-name>
                    image-spec(str): [<pool-name>/[<namespace>/]]<image-name>
                    See rbd help group image add for more supported keys
            """
            kw_copy = deepcopy(kw)
            group_spec = kw_copy.pop("group-spec", "")
            image_spec = kw_copy.pop("image-spec", "")
            cmd = f"{self.base_cmd} add {group_spec} {image_spec} {build_cmd_from_args(**kw_copy)}"

            return self.parent.execute_as_sudo(cmd=cmd)

        def list(self, **kw):
            """
            lists all image in a group of pool/[namespace], if pool is not given then list in default pool i.e rbd.
            Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str) : name of the pool into which namespace should be stored
                    namespace(str): namespace in the pool
                    group(str): group name where image to be listed
                    group-spec(str): [<pool-name>/[<namespace>/]]<group-name>
                    format(str): json format output of listing namespace
                    See rbd help group image ls/list for more supported keys
            """
            kw_copy = deepcopy(kw)
            group_spec = kw_copy.pop("group-spec", "")
            cmd = f"{self.base_cmd} list {group_spec} {build_cmd_from_args(**kw_copy)}"

            return self.parent.execute_as_sudo(cmd=cmd)

        def rm(self, **kw):
            """
            Remove an image in pool/[namespace]/[group]
            Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    group-pool(str) : name of the pool into which namespace should be deleted
                    group-namespace(str): namespace in the pool
                    group(str): group where image to be added
                    image-pool(str): image pool name
                    image-namespace(str): image namespace name
                    image(str): image name
                    pool(str): pool name unless overridden
                    group-spec(str): [<pool-name>/[<namespace>/]]<group-name>
                    image-spec(str): [<pool-name>/[<namespace>/]]<image-name>
                    See rbd help group image add for more supported keys
            """
            kw_copy = deepcopy(kw)
            group_spec = kw_copy.pop("group-spec", "")
            image_spec = kw_copy.pop("image-spec", "")
            cmd = f"{self.base_cmd} rm {group_spec} {image_spec} {build_cmd_from_args(**kw_copy)}"

            return self.parent.execute_as_sudo(cmd=cmd)

    class Snap(object):
        def __init__(self, parent, base_cmd):
            self.base_cmd = base_cmd + " snap"
            self.parent = parent

        def create(self, **kw):
            """
            Snap create for a group of images
            Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str) : name of the pool into which namespace should be stored,default pool is rbd
                    namespace(str): namespace in the pool
                    group(str): group name in the pool
                    snap(str): snap name to be created
                    group-spec(str): <pool-name>/[<namespace>/]]<group-name>@<snap-name>
                    See rbd help group snap create for more supported keys
            """
            kw_copy = deepcopy(kw)
            group_spec = kw_copy.pop("group-spec", "")
            cmd = (
                f"{self.base_cmd} create {group_spec} {build_cmd_from_args(**kw_copy)}"
            )

            return self.parent.execute_as_sudo(cmd=cmd)

        def list(self, **kw):
            """
            lists all snaps in a given pool/[namespace]/[group], if pool is not given then list in default pool i.e rbd.
            Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str) : name of the pool into which namespace should be stored
                    namespace(str): namespace in the pool
                    group(str): group name where snap to be listed
                    group-spec(str): <pool-name>/[<namespace>/]]<group-name>@<snap-name>
                    format(str): json format output of listing namespace
                    See rbd help group snap ls/list for more supported keys
            """
            kw_copy = deepcopy(kw)
            group_spec = kw_copy.pop("group-spec", "")
            cmd = f"{self.base_cmd} list {group_spec} {build_cmd_from_args(**kw_copy)}"

            return self.parent.execute_as_sudo(cmd=cmd)

        def info(self, **kw):
            """
            lists information for particular group snaps in a given pool.
            If pool is not given then list in default pool i.e rbd.
            Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str) : name of the pool where group is present
                    group(str): group name where snap to be listed
                    snap(str): group snapshot name to be listed
                    format(str): json format output of listing namespace
                    See rbd help group snap ls/list for more supported keys
            """
            kw_copy = kw
            group_spec = kw_copy.pop("group-spec", "")
            cmd = f"{self.base_cmd} info {group_spec} {build_cmd_from_args(**kw_copy)}"

            return self.parent.execute_as_sudo(cmd=cmd)

        def rm(self, **kw):
            """
            Snap rm for a group of images
            Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str) : name of the pool into which namespace should be stored,default pool is rbd
                    namespace(str): namespace in the pool
                    group(str): group name in the pool
                    snap(str): snap name to be deleted
                    group-spec(str): <pool-name>/[<namespace>/]]<group-name>@<snap-name>
                    See rbd help snap rm for more supported keys
            """
            kw_copy = deepcopy(kw)
            group_spec = kw_copy.pop("group-spec", "")
            cmd = f"{self.base_cmd} rm {group_spec} {build_cmd_from_args(**kw_copy)}"

            return self.parent.execute_as_sudo(cmd=cmd)

        def rename(self, **kw):
            """
            Renames the group snap in a [pool]/[namespace]<group>@<snap>
            Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str) : name of the pool into which namespace should be stored,default pool is rbd
                    namespace(str): namespace in the pool
                    group(str): group name in the pool
                    snap(str): snap to be renamed
                    dest-snap(str): destination snapshot name
                    group-snap-spec(str): <pool-name>/[<namespace>/]]<group-name>@<snap-name>
                    dest-snap(str): <snap-name>
            """
            kw_copy = deepcopy(kw)
            group_snap_spec = kw_copy.pop("group-snap-spec", "")
            dest_snap_spec = kw_copy.pop("dest-snap", "")
            cmd = f"{self.base_cmd} rename {group_snap_spec}  {dest_snap_spec} {build_cmd_from_args(**kw_copy)}"

            return self.parent.execute_as_sudo(cmd=cmd)

        def rollback(self, **kw):
            """
            Rollbacks the group images to snap for a [pool]/[namespace]<group>@<snap>
            Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
                Example::
                Supported keys:
                    pool(str) : name of the pool into which namespace should be stored,default pool is rbd
                    namespace(str): namespace in the pool
                    group(str): group name in the pool
                    snap(str): snap to be rollbacked to
                    group-snap-spec(str): <pool-name>/[<namespace>/]]<group-name>@<snap-name>
            """
            kw_copy = deepcopy(kw)
            group_snap_spec = kw_copy.pop("group-snap-spec", "")
            cmd = f"{self.base_cmd} rollback {group_snap_spec} {build_cmd_from_args(**kw_copy)}"

            return self.parent.execute_as_sudo(cmd=cmd)
