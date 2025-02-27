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
        go_cmds += "tar -C /usr/local -xzf go1.23.6.linux-amd64.tar.gz"
        cmd_list = [
            "cd /home/cephuser;git clone https://github.com/google/fscrypt",
            "sudo yum install -y pam-devel",
            "yum install -y m4",
            f"cd /home/cephuser;{go_cmds}",
            "export PATH=$PATH:/usr/local/go/bin",
            "cd /home/cephuser/fscrypt;make;sudo make install",
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
        out, _ = client.exec_command(
            sudo=True,
            cmd=cmd,
        )
        log.info(out)
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
            protector_id = self.metadata_ops(
                client, "create", "protector", mnt_pt, **protector_params
            )
            cmd += f" --protector={mnt_pt}:{protector_id}"

        if policy_id:
            cmd += f" --policy={mnt_pt}:{policy_id}"
        else:
            policy_params = {"protector_id": protector_id}
            policy_id = self.metadata_ops(
                client, "create", "policy", mnt_pt, **policy_params
            )
            cmd += f" --policy={mnt_pt}:{policy_id}"

        cmd += f" --unlock-with={mnt_pt}:{protector_id}"

        out, rc = client.exec_command(
            sudo=True,
            cmd=cmd,
        )
        exp_str = f'"{encrypt_path}" is now encrypted, unlocked, and ready for use.'
        if exp_str not in out:
            log.error("%s is not successfully encrypted", encrypt_path)
            return 1
        encrypt_params = {
            "protector_id": protector_id,
            "protector_source": source,
            "protector_name": name,
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
        out, rc = client.exec_command(
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
                cmd = f"echo y | fscrypt metadata create protector --source={source} --name={name} {mnt_pt}"
                if entity_params.get("user"):
                    cmd += f" --user={entity_params['user']}"
                if entity_params.get("key"):
                    cmd += f" --key={entity_params['key']}"
                out, rc = client.exec_command(
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
                out_list = out.split("\n")
                out_list2 = out_list[1].split("[Y/n]")
                id_str = re.findall(
                    r"^.*Policy (\w+) created on filesystem.*$", out_list2[1]
                )
                entity_id = list(id_str)[0]
            return entity_id

        def destroy():
            id = entity_params["id"]
            cmd = f"echo y | fscrypt metadata destroy --{entity}={mnt_pt}:{id} --force"
            out, rc = client.exec_command(
                sudo=True,
                cmd=cmd,
            )
            out.strip()
            exp_str = f"{id} deleted from filesystem"
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
            out, rc = client.exec_command(
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

    # HELPER ROUTINES #

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
