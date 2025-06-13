"""
This is cephfs fscrypt feature Utility module
It contains methods to run fscrypt cli options - setup,encrypt,lock,unlock,purge

"""

import random
import re
import string
import time

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


class FscryptUtils(object):
    def __init__(self, ceph_cluster):
        """
        FScrypt Utility object
        Args:
            ceph_cluster (ceph.ceph.Ceph): ceph cluster
        """

        self.ceph_cluster = ceph_cluster
        self.fs_util = FsUtils(ceph_cluster)

    def get_status(self, client, mnt_pt=None):
        """
        This method is required to fetch fscrypt status with or w/o mountpoint and return in dict format
        Params:
        Required:
        client - A client object to run ceph cmds
        Optional:
        mnt_pt - mountpoint whose fscryot status needs to be fetched, type - str
                 if not given, it will fetch status for all mount points of ceph type
        """
        fscrypt_info = {}
        if mnt_pt:
            cmd = f"fscrypt status {mnt_pt}"
            status_str = client.exec_command(
                sudo=True,
                cmd=cmd,
            )
            str(status_str).strip()
            fscrypt_info.update({mnt_pt: {}})
            status = str(status_str).split("\n")
            fscrypt_info[mnt_pt] = self.format_fscrypt_info(status)
        else:
            cmd = "fscrypt status"
            status = client.exec_command(
                sudo=True,
                cmd=cmd,
            )
            status_list = status.split("\n")

            for line in status_list:
                if "ceph" in line:
                    list1 = line.split()
                    mnt_pt = list1[0]
                    fscrypt_info.update({mnt_pt: {}})
                    cmd = f"fscrypt status {mnt_pt}"
                    status_str = client.exec_command(
                        sudo=True,
                        cmd=cmd,
                    )
                    status = status_str.split("\n")
                    fscrypt_info[mnt_pt] = self.format_fscrypt_info(status)
        return fscrypt_info

    def fscrypt_install(self, client):
        """
        This method is required to build and install fscrypt cli.
        Return 0 upon success and 1 for failure
        Params:
        Required:
        client - A client object to install fscrypt cli

        """
        go_cmds = "wget https://go.dev/dl/go1.23.6.linux-amd64.tar.gz;"
        go_cmds += "tar -C /usr/local -xzf go1.23.6.linux-amd64.tar.gz;sleep 5"
        fscrypt_cmds = "cd /home/cephuser/fscrypt;make;sudo make install"
        cmd_list = [
            "cd /home/cephuser;git clone -b wip-ceph-fuse https://github.com/ceph/fscrypt.git",
            "sudo yum install -y pam-devel",
            "yum install -y m4",
            f"cd /home/cephuser;{go_cmds}",
            f"export PATH=$PATH:/usr/local/go/bin;sleep 2;{fscrypt_cmds}",
        ]
        for cmd in cmd_list:
            try:
                out, _ = client.exec_command(
                    sudo=True,
                    cmd=cmd,
                )
                log.info(out)
            except BaseException as ex:
                log.info(ex)
        try:
            out, _ = client.exec_command(
                sudo=True,
                cmd=cmd,
            )
            log.info(out)
            return 0
        except BaseException as ex:
            if "command not found" in str(ex):
                return 1

    def setup(self, client, mnt_pt, validate=True):
        """
        This method is required to setup fscrypt on root if doesn't exist and also on given mountpoint.
        Return 0 upon success and 1 for failure
        Params:
        Required:
        client - A client object to run ceph cmds
        mnt_pt - mountpoint whose fscrypt status needs to be fetched, type - str
        Optional:
        Validate - if set to True(default), validates metadata directories created after setup
        """

        try:
            cmd = "fscrypt"
            client.exec_command(
                sudo=True,
                cmd=cmd,
            )
        except BaseException as ex:
            if "command not found" in str(ex):
                if self.fscrypt_install(client):
                    log.error("fscrypt install failed")
                    return 1

        def_conf = "/etc/fscrypt.conf"
        cmd = f"ls {def_conf}"
        try:
            client.exec_command(
                sudo=True,
                cmd=cmd,
            )
        except BaseException as ex:
            if "No such file" in str(ex):
                cmd = "echo y | fscrypt setup --force"
                client.exec_command(
                    sudo=True,
                    cmd=cmd,
                )
        cmd = f"echo y | fscrypt setup {mnt_pt}"
        try:
            out, _ = client.exec_command(
                sudo=True,
                cmd=cmd,
            )
            log.info(out)
        except BaseException as ex:
            if "already setup for use" in str(ex):
                log.info("FScrypt setup already exists for %s", mnt_pt)
            else:
                log.error("Unexpected error during fscrypt setup - %s", str(ex))
                return 1
        if validate:
            try:
                cmd = f"ls -l {mnt_pt}/.fscrypt/"
                out, rc = client.exec_command(
                    sudo=True,
                    cmd=cmd,
                )
                log.info(out)
                found = 0
                for item in ["policies", "protectors"]:
                    if item in out:
                        found += 1
                if found == 0:
                    log.error("Policies and Protectors were not created")
                    return 1
                elif found == 1:
                    log.error("Either policies or protectors not created")
                    return 1
            except BaseException as ex:
                if "No such file" in str(ex):
                    log.error(".fscrypt dir was not created")
                    return 1
        return 0

    def encrypt(
        self, client, encrypt_path, mnt_pt, policy_id=None, protector_id=None, **kwargs
    ):
        """
        This method is required to encrypt given path. Policy ID and Protector ID can given,
        else it will be created and shared with return variable for further use say during unlock.
        Params:
        Required -
        client - A client object to run ceph cmds
        encrypt_path - Path within mountpoint that needs to be encrypted, type - str
        mnt_pt - A mountpoint within which a path is being encrypted
        Optional-
        policy_id - A existing policy ID to be used for encrypt path, if None(default), it will be created
        protector_id - A existing protector ID to be used for encrypt path, if None(default), it will be created
        kwargs - Additional args could be as below,
        kwargs = {
        'protector_source' : 'custom_passphrase',
        'protector_name' : 'cephfs_1'}
        """
        rand_str = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(3))
        )
        cmd = f"echo y | fscrypt encrypt {encrypt_path}"
        if protector_id:
            cmd += f" --protector={mnt_pt}:{protector_id}"
        else:
            source = kwargs.get("protector_source", "custom_passphrase")
            name = kwargs.get("protector_name", f"cephfs_{rand_str}")
            protector_params = {"source": source, "name": name}
            # if source is pam_passphrase get 'user'
            if source == "pam_passphrase":
                if kwargs["user"]:
                    protector_params.update({"user": kwargs["user"]})
                else:
                    log.error(
                        "For protector source as pam_passphrase, 'user' param is required"
                    )
                    return 1
            elif source == "raw_key":
                if not kwargs.get("protector_key"):
                    key_path = f"{mnt_pt}/{name}.key"
                    cmd_1 = f"head --bytes=32 /dev/urandom > {key_path}"
                    client.exec_command(
                        sudo=True,
                        cmd=cmd_1,
                    )
                    protector_params.update({"key": key_path})
                else:
                    protector_params.update({"key": kwargs["protector_key"]})
            protector_id = self.metadata_ops(
                client, "create", "protector", mnt_pt, **protector_params
            )
            cmd += f" --protector={mnt_pt}:{protector_id}"
            if source == "raw_key":
                cmd += f" --key={protector_params['key']}"

        if policy_id:
            cmd += f" --policy={mnt_pt}:{policy_id}"
        else:
            policy_params = {
                "protector_id": protector_id,
                "key": protector_params.get("key", None),
            }
            policy_id = self.metadata_ops(
                client, "create", "policy", mnt_pt, **policy_params
            )
            cmd += f" --policy={mnt_pt}:{policy_id}"

        cmd += f" --unlock-with={mnt_pt}:{protector_id}"

        out, _ = client.exec_command(
            sudo=True,
            cmd=cmd,
        )
        exp_str = f'"{encrypt_path}" is now encrypted, unlocked, and ready for use.'
        if exp_str not in out:
            log.error("%s is not successfully encrypted", encrypt_path)
            return 1
        encrypt_params = {
            "protector_id": protector_id,
            "protector_source": protector_params.get("source", None),
            "protector_name": protector_params.get("name", None),
            "key": protector_params.get("key", None),
            "policy_id": policy_id,
        }
        return encrypt_params

    def lock(self, client, encrypt_path, **kwargs):
        """
        This method is required to lock the given encrypt path and return 0 upon success else 1.
        Params:
        Required:
        client - A client object to run ceph cmds
        encrypt_path - Path within mountpoint that needs to be encrypted, type - str
        Optional:
        kwargs : This includes params in below format,
        kwargs = {
        'user' : user,
        'all_users':True}
        user - Specify which user should be used for login passphrases or to which user's keyring keys
        should be provisioned
        all_users - Lock the directory no matter which user(s) have unlocked it. Requires root privileges.
                    This flag is only necessary if the directory was unlocked by a user different from the
                    one you're locking it as
        """
        cmd = f"fscrypt lock {encrypt_path}"
        if kwargs.get("user"):
            cmd += f" --user={kwargs['user']}"
        if kwargs.get("all_users"):
            cmd += " --all_users"
        not_successful = 1
        retry_cnt = 0
        while not_successful and retry_cnt < 3:
            try:
                out, _ = client.exec_command(
                    sudo=True,
                    cmd=cmd,
                )
                out = out.strip()
                exp_str = "is now locked"
                exp_str_1 = "is already locked"
                if (exp_str not in out) and (exp_str_1 not in out):
                    log.error("Lock on %s seems not succesful:%s", encrypt_path, out)
                    return 1
                not_successful = 0
            except BaseException as ex:
                log.info(ex)
                exp_str_2 = "Directory was incompletely locked because some files are"
                if exp_str_2 in str(ex):
                    try:
                        cmd_1 = f'find "{encrypt_path}" -print0 | xargs -0 fuser -k'
                        out, _ = client.exec_command(
                            sudo=True,
                            cmd=cmd_1,
                        )
                        log.info(out)
                    except BaseException as ex1:
                        log.info(ex1)
                    time.sleep(30)
                retry_cnt += 1
        if not_successful:
            log.error("Even after multiple retries,Lock was not successful")
            return 1
        return 0

    def unlock(self, client, encrypt_path, mnt_pt, protector_id, **kwargs):
        """
        This method is required to unlock the given encrypt path and return 0 upon success else 1.
        Params:
        Required:
        client - A client object to run ceph cmds
        encrypt_path - Path within mountpoint that needs to be encrypted, type - str
        mnt_pt - Mountpoint required as we need to pass protector param value inline as mnt_pt:ID
        protector_id - Protector ID to unlock unecrypt path
        Optional:
        kwargs : This includes params in below format,
        kwargs = {
        'key' : keyring_file_path,
        'user':user}
        user - Specify which user should be used for login passphrases or to which user's keyring keys
        should be provisioned
        key - key ring file path, To use the contents of FILE as the wrapping key when creating or unlocking
              raw_key protectors.FILE should be formatted as raw binary and should be exactly 32 bytes long.
        """
        cmd = f"echo y|fscrypt unlock {encrypt_path} --unlock-with={mnt_pt}:{protector_id}"
        if kwargs.get("key"):
            cmd += f" --key {kwargs['key']}"
        if kwargs.get("user"):
            cmd += f" --user {kwargs['user']}"
        out, _ = client.exec_command(
            sudo=True,
            cmd=cmd,
        )
        out.strip()
        exp_str = "is now unlocked and ready for use"
        if exp_str not in out:
            log.error("Unlock on %s seems not succesful:%s", encrypt_path, out)
            return 1
        return 0

    def purge(self, client, mnt_pt, validate=True, **kwargs):
        """
        This method is required to Purge all policy keys from mnt_pt
        Params:
        Required:
        client - A client object to run ceph cmds
        mnt_pt- A mountpoint within which all policy keys to be purged, type - str
        Optional:
        kwargs : This includes params in below format,
        kwargs = {
        'force' : True,
        'user':user}
        user - Specify which user should be used for login passphrases or to which user's keyring keys
        should be provisioned
        """
        cmd = f"echo y | fscrypt purge {mnt_pt}"
        if kwargs.get("force"):
            cmd += " --force"
        if kwargs.get("user"):
            cmd += f" --user {kwargs['user']}"
        out, _ = client.exec_command(
            sudo=True,
            cmd=cmd,
        )
        log.info(out)
        if validate:
            exp_str = "Policies purged"
            fscrypt_status = self.get_status(client, mnt_pt)
            log.info(fscrypt_status)
            if exp_str not in out:
                log.error("Purge not suceessful on %s", mnt_pt)
                return 1
        return 0

    def metadata_ops(self, client, op_name, entity, mnt_pt, **entity_params):
        """
        This method is required to run create,destroy and other metadata ops on policies and protectors
        ops supported in this method - create,destroy,add_protector_to_policy,remove_protector_from_policy
        Params:
        Required:
        client - A client object to run ceph cmds
        mnt_pt- A mountpoint within which all metadata ops needs to be performed, type - str
        op_name - one of ops (create,destroy,add_protector_to_policy,remove_protector_from_policy)
        entity - policy or protector which needs to be created, destroy, added or removed
        Optional:
        kwargs : This includes params in below format,
        kwargs = {
        'key' : keyring_file_path,
        'user':user,
        'source' : custom_passphrase,
        'name':cephfs,
        'protector_id' : protector_id,
        'id' : ID,
        'policy_id': policy_id}
        user - Specify which user should be used for login passphrases or to which user's keyring keys
        should be provisioned
        key - key ring file path, To use the contents of FILE as the wrapping key when creating or unlocking
        raw_key protectors.FILE should be formatted as raw binary and should be exactly 32 bytes long.
        ID - used for destroy op, it needs to protector ID if entity is protector, else policy ID
        user - Specify which user should be used for login passphrases or to which user's keyring keys
        should be provisioned
        """

        def create():
            if entity == "protector":
                source = entity_params["source"]
                name = entity_params["name"]
                cmd = f"echo y | fscrypt metadata create protector {mnt_pt} --source={source}"
                if "pam_passphrase" not in source:
                    cmd += f" --name={name} "
                elif entity_params.get("user"):
                    cmd += f" --user={entity_params['user']}"
                if "raw_key" in source:
                    cmd += f" --key={entity_params['key']}"
                out, _ = client.exec_command(
                    sudo=True,
                    cmd=cmd,
                )
                out.strip()
                log.info(out)
                id_str = re.findall(r"^.*Protector (\w+) created on filesystem.*$", out)
                entity_id = list(id_str)[0]

            elif entity == "policy":
                protector_id = entity_params["protector_id"]
                cmd = f"echo y|fscrypt metadata create policy {mnt_pt} --protector={mnt_pt}:{protector_id}"
                if entity_params.get("key"):
                    cmd += f" --key={entity_params['key']}"
                out, rc = client.exec_command(
                    sudo=True,
                    cmd=cmd,
                )
                log.info(out)
                out_list = out.split("[Y/n]")
                id_str = re.findall(
                    r"^.*Policy (\w+) created on filesystem.*$", out_list[1]
                )
                entity_id = list(id_str)[0]
            return entity_id

        def destroy():
            id = entity_params["id"]
            if entity_params.get("id"):
                cmd = f"echo y | fscrypt metadata destroy --{entity}={mnt_pt}:{id} --force"
                out, _ = client.exec_command(
                    sudo=True,
                    cmd=cmd,
                )
                out.strip()
                exp_str = f"{id} deleted from filesystem"
                if exp_str not in str(out):
                    log.error("%s %s deletion seems not successful:%s", entity, id, out)
                    return 1
            else:
                cmd = f"echo y | fscrypt metadata destroy {mnt_pt}"
                out, rc = client.exec_command(
                    sudo=True,
                    cmd=cmd,
                )
                out.strip()
                exp_str = f'All metadata on "{mnt_pt}" deleted'
                if exp_str not in str(out):
                    log.error("%s %s deletion seems not successful:%s", entity, id, out)
                    return 1
            return 0

        def add_protector_to_policy():
            pro_id = entity_params["protector_id"]
            pol_id = entity_params["policy_id"]
            cmd = f"echo y | fscrypt metadata add-protector-to-policy --protector={mnt_pt}:{pro_id}"
            cmd += f" --policy={mnt_pt}:{pol_id} --unlock-with={mnt_pt}:{pro_id}"
            out, rc = client.exec_command(
                sudo=True,
                cmd=cmd,
            )
            log.info(out)
            return 0

        def remove_protector_from_policy():
            pro_id = entity_params["protector_id"]
            pol_id = entity_params["policy_id"]
            cmd = f"echo y | fscrypt metadata remove-protector-from-policy --protector={mnt_pt}:{pro_id}"
            cmd += f" --policy={mnt_pt}:{pol_id}"
            out, _ = client.exec_command(
                sudo=True,
                cmd=cmd,
            )
            log.info(out)
            return 0

        metadata_ops_obj = {
            "create": create,
            "destroy": destroy,
            "add_protector_to_policy": add_protector_to_policy,
            "remove_protector_from_policy": remove_protector_from_policy,
        }
        entity_id = metadata_ops_obj[op_name]()
        return entity_id

    def validate_fscrypt(self, client, encrypt_mode, encrypt_path):
        """
        This method is required to validate file ops that should suceed in lock and in unlock mode
        Params:
        Required:
        client - A client object to run ceph cmds
        encrypt_mode - lock or unlock
        encrypt_path - encrypted path in mountpoint to be verified
        return 0 if validation suceeds for given encrypt mode else return 1
        """
        file_ops = [
            "name_content_read",
            "create",
            # "file_open",
            "write_overwrite",
            "truncate",
            "append",
            "rename",
        ]
        all_file_ops = file_ops.copy()
        all_file_ops.append("delete")
        ops = {
            "lock": {"not_allowed": file_ops, "allowed": ["delete"]},
            "unlock": {"not_allowed": [], "allowed": all_file_ops},
        }
        ops_to_test = ops[encrypt_mode]
        lock_str = "Required key not available"

        def name_content_read():
            cmd = f"find {encrypt_path} -type f"
            out, _ = client.exec_command(
                sudo=True,
                cmd=cmd,
            )
            files_list = out.split("\n")
            sample_cnt = min(5, len(files_list))
            files_list_1 = random.sample(files_list, sample_cnt)
            cmd = f"find {encrypt_path} -type d"
            out, _ = client.exec_command(
                sudo=True,
                cmd=cmd,
            )
            dir_list = out.split("\n")
            sample_cnt = min(5, len(dir_list))
            dir_list_1 = random.sample(dir_list, sample_cnt)
            dir_list.pop(0)
            file_read = 0
            file_locked = 0
            try:
                for file_path in files_list_1:
                    if file_path:
                        cmd = f"dd if={file_path} bs=4k count=2 > /var/tmp/tmp_read.log"
                        client.exec_command(
                            sudo=True,
                            cmd=cmd,
                        )
                file_read = 1
            except BaseException as ex:
                log.info(ex)
                if lock_str in str(ex):
                    file_locked = 1
            encrypted_files = []
            encrypted_dirs = []
            file_dir_names_encrypted = 0
            for file_path in files_list_1:
                cmd = f"basename {file_path}| wc -c"
                out, _ = client.exec_command(sudo=True, cmd=cmd)
                charcount = out.strip()
                if charcount == 44:
                    encrypted_files.append(file_path)
            for dir_path in dir_list_1:
                cmd = f"basename {dir_path}| wc -c"
                out, _ = client.exec_command(sudo=True, cmd=cmd)
                if charcount == 44:
                    encrypted_dirs.append(dir_path)
            if len(encrypted_files) == len(files_list_1) and len(encrypted_dirs) == len(
                dir_list_1
            ):
                file_dir_names_encrypted = 1
            if encrypt_mode == "lock":
                if (file_locked and (file_read == 0)) and file_dir_names_encrypted:
                    return 0
                else:
                    return 1
            elif encrypt_mode == "unlock":
                if ((file_locked == 0) and file_read) and (
                    file_dir_names_encrypted == 0
                ):
                    return 0
                else:
                    return 1

        def file_create():
            cmd = f"echo cephfs_fscrypt_testing > {encrypt_path}/testfile"
            try:
                client.exec_command(sudo=True, cmd=cmd)
            except BaseException as ex:
                log.info(ex)
                return 1
            return 0

        """def file_open():
            test_files_list = self.get_file_list(client,encrypt_path)
            file_to_open = random.choice(test_files_list)
            try:
                fh_fscrypt = open(file_to_open, "r")
                fh_fscrypt.readlines()
                fh_fscrypt.close()
            except BaseException as ex:
                log.error(ex)
                return 1
            return 0
        """

        def write_overwrite():
            test_files_list = self.get_file_list(client, encrypt_path)
            file_to_overwrite = random.choice(test_files_list)
            cmd = f"cp /var/log/messages {file_to_overwrite}"
            try:
                client.exec_command(sudo=True, cmd=cmd)
            except BaseException as ex:
                log.info(ex)
                return 1
            return 0

        def truncate():
            test_files_list = self.get_file_list(client, encrypt_path)
            file_to_truncate = random.choice(test_files_list)
            cmd = f"truncate -s -1M {file_to_truncate}"
            try:
                client.exec_command(sudo=True, cmd=cmd)
            except BaseException as ex:
                log.info(ex)
                return 1
            return 0

        def append():
            test_files_list = self.get_file_list(client, encrypt_path)
            file_to_append = random.choice(test_files_list)
            cmd = f"echo cephfs_fscrypt_testing >> {file_to_append}"
            try:
                client.exec_command(sudo=True, cmd=cmd)
            except BaseException as ex:
                log.info(ex)
                return 1
            return 0

        def rename():
            test_files_list = self.get_file_list(client, encrypt_path)
            file_to_rename, file_to_rename_1 = random.sample(test_files_list, 2)
            retry_cnt = 5
            while retry_cnt:
                if ("renamed_file" in file_to_rename) or (
                    "renamed_file" in file_to_rename_1
                ):
                    file_to_rename, file_to_rename_1 = random.sample(test_files_list, 2)
                    retry_cnt -= 1
                else:
                    retry_cnt = 0
            try:
                client.exec_command(
                    sudo=True, cmd=f"mv {file_to_rename} {encrypt_path}/renamed_file"
                )
                client.exec_command(
                    sudo=True,
                    cmd=f"mv {file_to_rename_1} {encrypt_path}/../renamed_file",
                )
            except BaseException as ex:
                log.info(ex)
                return 1
            return 0

        def delete():
            test_files_list = self.get_file_list(client, encrypt_path)
            file_to_del = random.choice(test_files_list)
            try:
                client.exec_command(sudo=True, cmd=f"rm -f {file_to_del}")
            except BaseException as ex:
                log.info(ex)
                return 1
            return 0

        failed_ops = []
        ops_func = {
            "name_content_read": name_content_read(),
            "create": file_create(),
            # "file_open": file_open(),
            "write_overwrite": write_overwrite(),
            "truncate": truncate(),
            "append": append(),
            "rename": rename(),
            "delete": delete(),
        }
        for file_op in all_file_ops:
            test_status = ops_func[file_op]
            if (
                test_status == 1
                and "name_content_read" in file_op
                and file_op in ops_to_test["allowed"]
            ):
                failed_ops.append(file_op)
            elif test_status == 1 and file_op in ops_to_test["allowed"]:
                failed_ops.append(file_op)
            elif test_status == 0 and file_op in ops_to_test["not_allowed"]:
                failed_ops.append(file_op)
        if len(failed_ops) > 0:
            log.error(
                "Some of the File ops failed in %s which was not expected:%s",
                encrypt_mode,
                failed_ops,
            )
        return_val = 1 if len(failed_ops) > 0 else 0
        return return_val

    def validate_fscrypt_metadata(self, client, encrypt_path):
        test_files_list = self.get_file_list(client, encrypt_path)
        test_file = random.choice(test_files_list)
        out, _ = client.exec_command(sudo=True, cmd=f"ls -l {test_file}")
        ls_out = out.split("\n")
        if "total" in ls_out[0]:
            ls_out.pop(0)
        ls_list = re.split(r"\s+", ls_out[0])
        test_status = 0

        def check_perm(column):
            """
            This method checks if permissions field of ls output is encrypted
            return - 1 if encrypted, 0 if not encrypted
            """
            found = 0
            pattern = ["d", "-", "r", "w", "x", "."]
            for i in range(0, len(ls_list[column])):
                if ls_list[column][i] in pattern:
                    found += 1
            if found == len(ls_list[column]):
                log.info("File permissions not encrypted")
                return 0
            else:
                log.error("File permissions could be encrypted:%s", ls_list[column])
                return 1

        def check_links_size_date(column):
            """
            This method checks if soft/hard link field, size or date field of ls output is encrypted
            return - 1 if encrypted, 0 if not encrypted
            """
            info = {1: "Number of links", 4: "Size_in_bytes", 6: "Date"}
            res = re.search(r"(\d+)", ls_list[column])
            if res:
                log.info("%s info is not encrypted", info[column])
                return 0
            else:
                log.error(
                    "%s info could be encrypted:%s", info[column], ls_list[column]
                )
                return 1

        def check_user_group_info(column):
            """
            This method checks if user and group field of ls output is encrypted
            return - 1 if encrypted, 0 if not encrypted
            """
            cmd = "awk -F':' '{ print $1}' /etc/passwd"
            out, _ = client.exec_command(sudo=True, cmd=cmd)
            user_list = out.split("\n")

            if ls_list[column] in user_list:
                log.info("User/group info is not encrypted")
                return 0
            else:
                log.error("User/group info could be encrypted:%s", ls_list[column])
                return 1

        def check_month(column):
            """
            This method checks if month field of ls output is encrypted
            return - 1 if encrypted, 0 if not encrypted
            """
            pattern = [
                "Jan",
                "Feb",
                "Mar",
                "Apr",
                "May",
                "Jun",
                "Jul",
                "Aug",
                "Sep",
                "Oct",
                "Nov",
                "Dec",
            ]
            if ls_list[column] in pattern:
                log.info("Month info is not encrypted")
                return 0
            else:
                log.error("Month info could be encrypted:%s", ls_list[column])
                return 1

        def check_time(column):
            """
            This method checks if time field of ls output is encrypted
            return - 1 if encrypted, 0 if not encrypted
            """
            res = re.search(r"(\d+):(\d+)", ls_list[column])
            if res:
                log.info("Time info is not encrypted")
                return 0
            else:
                log.error("Time info could be encrypted:%s", ls_list[column])
                return 1

        lsop_test = {
            "0": check_perm,
            "146": check_links_size_date,
            "23": check_user_group_info,
            "5": check_month,
            "7": check_time,
        }
        for i in range(0, (len(ls_list) - 1)):
            if i in [1, 4, 6]:
                test_status += lsop_test["146"](i)
            elif i in [2, 3]:
                test_status += lsop_test["23"](i)
            else:
                test_status += lsop_test[str(i)](i)

        cmd = f"getfattr -n security.selinux {test_file}"
        out, _ = client.exec_command(sudo=True, cmd=cmd)
        if "security.selinux" in out:
            log.info("Extended attribute security.selinux not encrypted")
        else:
            log.error("Extended attribute security.selinux could be encrypted:%s", out)
            test_status += 1
        return test_status

    def encrypt_dir_setup(self, mount_details):
        """
        This method assists in creating a encrypted directory under given mountpoint
        Required params:
        mount_details - A dict object which is a return variable from cephfs_common_lib.test_mount
        including mount details for each subvol
        """
        encrypt_info = {}
        for sv_name in mount_details:
            encrypt_info.update({sv_name: {}})
            mnt_pt = mount_details[sv_name]["fuse"]["mountpoint"]
            mnt_client = mount_details[sv_name]["fuse"]["mnt_client"]
            if self.setup(mnt_client, mnt_pt):
                return 1
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(4))
            )
            encrypt_path = f"{mnt_pt}/testdir_{rand_str}"
            encrypt_info[sv_name].update({"path": encrypt_path})
            cmd = f"mkdir {encrypt_path}"
            mnt_client.exec_command(
                sudo=True,
                cmd=cmd,
                check_ec=False,
            )
            encrypt_args = {
                "protector_source": random.choice(["custom_passphrase", "raw_key"])
            }
            log.info("fscrypt encrypt on test directory")
            encrypt_params = self.encrypt(
                mnt_client, encrypt_path, mnt_pt, **encrypt_args
            )
            encrypt_info[sv_name].update({"encrypt_params": encrypt_params})
        return encrypt_info

    def is_encrypted(self, file_path, old_file_path, mnt_client):
        """
        This helper validates if given file_path is encrypted by comparing the file chars with old_file_path
        Params:
        file_path - current file after fscrypt lock
        old_file_path - file before fscrypt lock
        Returns:
        True if encrypted, False if not encrypted
        """
        cmd = f"basename {file_path}| wc -c"
        out, _ = mnt_client.exec_command(sudo=True, cmd=cmd)
        charcount_new = out.strip()
        cmd = f"basename {old_file_path}| wc -c"
        out, _ = mnt_client.exec_command(sudo=True, cmd=cmd)
        charcount_old = out.strip()
        log.info(charcount_new)
        log.info(charcount_old)
        if int(charcount_new) != int(charcount_old):
            return True
        return False

    def verify_encryption(self, mnt_client, encrypt_path, old_file_list, sampling=True):
        """
        This helper validates all files in given encrypt_path for encrypted filename, returns 0 if encrypted else 1
        Params:
        mnt_client - Client object
        encrypt_path - Path within which files to be verified for encryption
        old_file_list - List of files before fscrypt lock
        Returns:
        0 if all files in given path is encrypted, else 1
        """
        files_list = self.get_file_list(mnt_client, encrypt_path)
        file_cnt = len(files_list)
        if sampling:
            file_cnt = min(5, len(files_list))
        encrypted_files = []
        for i in range(0, file_cnt):
            if self.is_encrypted(files_list[i], old_file_list[i], mnt_client):
                encrypted_files.append(files_list[i])
            i += 1
        if len(encrypted_files) != file_cnt:
            log.info(
                "Some files are not encrypted, Encrypted - %s, Expected - %s",
                len(encrypted_files),
                len(files_list),
            )
            return 1
        return 0

    def validate_fscrypt_with_lock_unlock(
        self, mnt_client, mnt_pt, encrypt_path, encrypt_params
    ):
        """
        This method validates file encryption in lock and unlock state.
        Required:
        mnt_client : Client object of mountpoint
        mnt_pt : FScrypt setup mountpoint
        encrypt_path : Encrypted path to be validated
        encrypt_params : Dict object with encryption params as below,
        encrypt_params : { 'protector_id':protector_id,
                           'key' : key}
        Returns:
        0 if suceeds,1 if fails
        """
        validate_status = 0
        file_list = self.get_file_list(mnt_client, encrypt_path)
        log.info("Fscrypt lock on %s", encrypt_path)
        validate_status = self.lock(mnt_client, encrypt_path)
        validate_status += self.verify_encryption(mnt_client, encrypt_path, file_list)
        unlock_args = {"key": encrypt_params["key"]}
        protector_id = encrypt_params["protector_id"]
        log.info("FScrypt unlock on %s", encrypt_path)
        validate_status += self.unlock(
            mnt_client, encrypt_path, mnt_pt, protector_id, **unlock_args
        )
        if not self.verify_encryption(mnt_client, encrypt_path, file_list):
            log.error("Files are encrypted after unlock")
            validate_status += 1
        if validate_status > 0:
            log.error("FScrypt verify on mountpoint %s failed", mnt_pt)
            return 1
        return 0

    # HELPER ROUTINES #
    def get_file_list(self, client, path):
        """
        This helper method is to provide list of files in path
        Returns:
        files in list format
        """
        cmd = f"find {path} -maxdepth 1 -type f"
        out, _ = client.exec_command(sudo=True, cmd=cmd)
        test_files = out.strip()
        test_files_list = test_files.split("\n")
        return test_files_list

    def format_fscrypt_info(self, status):
        fscrypt_info = {}

        list1 = status[0].split()

        protector_cnt = list1[4]
        policy_cnt = list1[7]
        fscrypt_info.update({"protector_cnt": protector_cnt, "policy_cnt": policy_cnt})
        if policy_cnt == 0:
            return fscrypt_info
        if len(status) > 3:
            fscrypt_info.update({"policies": {}, "protectors": {}})
            sub_status = status.copy()

            for i in range(0, 4):
                sub_status.pop(0)

            protector = 1
            for line in sub_status:
                if ("POLICY" not in line) and protector:
                    list2 = line.split()
                    if len(list2) > 0:
                        protector_id = list2[0]
                        x = re.findall(
                            r"^\s+(\w+)\s+(\w+)\s+(\w+.*)\"(\w+)\".*$", status[4]
                        )
                        x_list = list(x[0])
                        protector_type = x_list[2]
                        protector_name = x_list[3]
                        fscrypt_info["protectors"].update(
                            {
                                protector_id: {
                                    "type": protector_type,
                                    "name": protector_name,
                                }
                            }
                        )
                elif "POLICY" in line:
                    protector = 0
                    continue
                if protector == 0:
                    policy_id, unlocked, protector_id = line.split()
                    fscrypt_info["policies"].update(
                        {
                            policy_id: {
                                "unlocked": unlocked,
                                "protector_id": protector_id,
                            }
                        }
                    )
        return fscrypt_info

    def add_dataset(self, client, encrypt_path, **kwargs):
        """
        This method is to add add files using dd to directory path created as - mix(depth-10,breadth-5)
        Also to add some test files used for validation in lock/unlock states
        """
        file_list = []

        log.info("Add directory with multi-level breadth and depth")
        dir_path = f"{encrypt_path}/"

        # multi-depth
        for i in range(1, 11):
            dir_path += f"dir_{i}/"
        cmd = f"mkdir -p {dir_path}"
        client.exec_command(sudo=True, cmd=cmd)
        for i in range(2):
            file_path = f"{dir_path}dd_file_2m_{i}"
            file_list.append(file_path)
        # multi-breadth
        dir_path = f"{encrypt_path}"
        for i in range(2, 6):
            cmd = f"mkdir {dir_path}/dir_{i}/"
            client.exec_command(sudo=True, cmd=cmd)
            for j in range(2):
                file_path = f"{dir_path}/dir_{i}/dd_file_2m_{j}"
                file_list.append(file_path)

        for file_path in file_list:
            client.exec_command(
                sudo=True,
                cmd=f"dd bs=1M count=2 if=/dev/urandom of={file_path}",
            )

        log.info("Add directory with files used for lock/unlock tests")
        linux_files = [
            "/var/log/messages",
            "/var/log/cloud-init.log",
            "/var/log/dnf.log",
        ]
        file_cnt = kwargs.get("extra_files", 5)
        for i in range(1, file_cnt + 1):
            linux_file = random.choice(linux_files)
            file_path = f"{encrypt_path}/testfile_{i}"
            client.exec_command(
                sudo=True,
                cmd=f"dd bs=1M count=2 if={linux_file} of={file_path}",
            )

        return file_list
