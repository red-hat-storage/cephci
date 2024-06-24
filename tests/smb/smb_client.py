import io

from smb import base, smb_structs
from smb.SMBConnection import SMBConnection


class SMBClient:
    """Use pysmb to access the SMB server"""

    def __init__(self, server, share, username, passwd, port):
        self.server = server
        self.share = share
        self.username = username
        self.password = passwd
        self.port = port
        self.connected = False
        self.connect()

    def connect(self):
        try:
            self.ctx = SMBConnection(
                self.username,
                self.password,
                "smbclient",
                self.server,
                use_ntlm_v2=True,
            )
            self.ctx.connect(self.server, self.port)
            self.connected = True
        except base.SMBTimeout as error:
            raise IOError(f"failed to connect: {error}")

    def disconnect(self):
        self.connected = False
        try:
            self.ctx.close()
        except base.SMBTimeout as error:
            raise TimeoutError(f"disconnect: {error}")

    def listdir(self, path="/"):
        try:
            dentries = self.ctx.listPath(self.share, path)
        except smb_structs.OperationFailure as error:
            raise IOError(f"failed to readdir: {error}")
        except base.SMBTimeout as error:
            raise TimeoutError(f"listdir: {error}")
        except base.NotConnectedError as error:
            raise ConnectionError(f"listdir: {error}")

        return [dent.filename for dent in dentries]

    def mkdir(self, dpath):
        try:
            self.ctx.createDirectory(self.share, dpath)
        except smb_structs.OperationFailure as error:
            raise IOError(f"failed to mkdir: {error}")
        except base.SMBTimeout as error:
            raise TimeoutError(f"mkdir: {error}")
        except base.NotConnectedError as error:
            raise ConnectionError(f"mkdir: {error}")

    def rmdir(self, dpath):
        try:
            self.ctx.deleteDirectory(self.share, dpath)
        except smb_structs.OperationFailure as error:
            raise IOError(f"failed to rmdir: {error}")
        except base.SMBTimeout as error:
            raise TimeoutError(f"rmdir: {error}")
        except base.NotConnectedError as error:
            raise ConnectionError(f"rmdir: {error}")

    def unlink(self, fpath):
        try:
            self.ctx.deleteFiles(self.share, fpath)
        except smb_structs.OperationFailure as error:
            raise IOError(f"failed to unlink: {error}")
        except base.SMBTimeout as error:
            raise TimeoutError(f"unlink: {error}")
        except base.NotConnectedError as error:
            raise ConnectionError(f"unlink: {error}")

    def write(self, fpath, writeobj):
        try:
            self.ctx.storeFile(self.share, fpath, writeobj)
        except smb_structs.OperationFailure as error:
            raise IOError(f"failed in write_text: {error}")
        except base.SMBTimeout as error:
            raise TimeoutError(f"write_text: {error}")
        except base.NotConnectedError as error:
            raise ConnectionError(f"write: {error}")

    def read(self, fpath, readobj):
        try:
            self.ctx.retrieveFile(self.share, fpath, readobj)
        except smb_structs.OperationFailure as error:
            raise IOError(f"failed in read_text: {error}")
        except base.SMBTimeout as error:
            raise TimeoutError(f"read_text: {error}")
        except base.NotConnectedError as error:
            raise ConnectionError(f"read: {error}")

    def write_text(self, fpath, teststr):
        with io.BytesIO(teststr.encode()) as writeobj:
            self.write(fpath, writeobj)

    def read_text(self, fpath):
        with io.BytesIO() as readobj:
            self.read(fpath, readobj)
            ret = readobj.getvalue().decode("utf8")
        return ret
