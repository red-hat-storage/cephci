from copy import deepcopy

from cli.utilities.utils import build_cmd_args


class Migration:

    """
    This Class provides wrappers for rbd migration commands.
    """

    def __init__(self, nodes, base_cmd):
        self.base_cmd = base_cmd + " migration"

    def abort(self, **kw):
        """Wrapper for rbd migration abort.

        Cancel interrupted image migration.

        Args:
            kw: Key value pair of method arguments
        Example::
        Supported keys:
            pool(str): Name of the pool of which peer is to be bootstrapped.
            namespace(str): name of the namespace
            image(str): image name
            no-progress(bool): True - disable progress output
            image_spec(str): image specification

        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} abort {image_spec}{cmd_args}"

        return self.execute(cmd=cmd)

    def commit(self, **kw):
        """Wrapper for rbd migration commit.

        Commit image migration.

        Args:
            kw: Key value pair of method arguments
        Example::
        Supported keys:
            pool(str): Name of the pool of which peer is to be bootstrapped.
            namespace(str): name of the namespace
            image(str): image name
            no-progress(bool): True - disable progress output
            force(bool): True - proceed even if the image has children
            image_spec(str): image specification

        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} commit {image_spec}{cmd_args}"

        return self.execute(cmd=cmd)

    def execute(self, **kw):
        """Wrapper for rbd migration abort.

        Execute image migration.

        Args:
            kw: Key value pair of method arguments
        Example::
        Supported keys:
            pool(str): Name of the pool of which peer is to be bootstrapped.
            namespace(str): name of the namespace
            image(str): image name
            no-progress(bool): True - disable progress output
            image_spec(str): image specification

        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        image_spec = kw_copy.pop("image_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} execute {image_spec}{cmd_args}"

        return self.execute(cmd=cmd)

    def prepare(self, **kw):
        """Wrapper for rbd migration abort.

        Prepare image migration.

        Args:
            kw: Key value pair of method arguments
        Example::
        Supported keys:
            import-only(bool): only import data from source
            source-spec-path(str): source-spec file
            source-spec(str): source-spec
            pool(str): name of the pool
            namespace(str): source namespace name
            image(str): source image name
            snap(str):  source snapshot name
            dest-pool(str): destination pool name
            dest-namespace(str):  destination namespace name
            dest(str):  destination image name
            image-format(str):  image format [default: 2]
            object-size(str): object size in B/K/M [4K <= object size <= 32M]
            image-feature(str): image features
            image-shared(bool): shared image
            stripe-unit(str): stripe unit in B/K/M
            stripe-count(str):  stripe count
            data-pool(str): data pool
            mirror-image-mode(str): mirror image mode [journal or snapshot]
            journal-splay-width(str): number of active journal objects
            journal-object-size(str): size of journal objects [4K <= size <= 64M]
            journal-pool(str):  pool for journal objects
            flatten(bool): True - fill clone with parent data (make it independent)
            source-image-or-snap-spec(str): source image or snapshot specification
            dest-image-spec: destination image specification
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        source_spec = kw_copy.pop("source-image-or-snap-spec", "")
        destination_spec = kw_copy.pop("dest-image-spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} prepare {source_spec} {destination_spec}{cmd_args}"

        return self.execute(cmd=cmd)
