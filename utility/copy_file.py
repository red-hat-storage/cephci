from utility.log import Log

log = Log(__name__)


class CopyFile:
    @staticmethod
    def run(kw, args):
        """Copy the file from one instance to another.

        Copy the specified file from source to destination instance.
        Note: This method is intended for small files for now.

        Usage
        - copy_file:
          args:
            copy_file: True
            source: ceph-rbd1
            dest: ceph-rbd2
            path: ./dummy_file
        """

        cluster = kw["ceph_cluster_dict"][args["source"]]

        for node in cluster:
            if node.role == "client":
                source_node = node
                break

        cmd = f"cat {args['path']}"
        contents, err = source_node.exec_command(sudo=True, cmd=cmd)

        cluster = kw["ceph_cluster_dict"][args["dest"]]
        for node in cluster:
            if node.role == "client":
                dest_node = node
                break

        key_file = dest_node.remote_file(
            sudo=True, file_name=args["file_name"], file_mode="w"
        )
        key_file.write(contents)
        key_file.flush()
