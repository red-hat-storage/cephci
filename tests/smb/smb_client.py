import uuid

import smbclient
from smbprotocol.connection import Connection
from smbprotocol.exceptions import SMBException

rw_chunk_size = 1 << 21  # 2MB


class SMBClient:
    """Use pysmb to access the SMB server"""

    def __init__(self, server, share, username, passwd, port):
        self.server = server
        self.share = share
        self.username = username
        self.password = passwd
        self.port = port
        self.connected = False
        self.connection_cache: dict = {}
        self.client_params = {
            "username": self.username,
            "password": self.password,
            "connection_cache": self.connection_cache,
        }
        self.prepath = f"\\\\{self.server}\\{self.share}\\"
        self.connect()

    def path(self, path: str = "/") -> str:
        path.replace("/", "\\")
        return self.prepath + path

    def connect(self):
        try:
            connection_key = f"{self.server.lower()}:{self.port}"
            connection = Connection(uuid.uuid4(), self.server, self.port)
            connection.connect()
            self.connection_cache[connection_key] = connection
            smbclient.register_session(
                self.server, port=self.port, **self.client_params
            )
            self.connected = True
        except SMBException as error:
            raise IOError(f"failed to connect: {error}")

    def disconnect(self):
        self.connected = False
        smbclient.reset_connection_cache(connection_cache=self.connection_cache)

    def check_connected(self, action):
        if not self.connected:
            raise ConnectionError(f"{action}: server not connected")

    def listdir(self, path="/"):
        self.check_connected("listdir")
        try:
            filenames = smbclient.listdir(self.path(path), **self.client_params)
        except SMBException as error:
            raise IOError(f"listdir: {error}")
        return filenames

    def mkdir(self, dpath):
        self.check_connected("mkdir")
        if not self.connected:
            raise ConnectionError("listdir: server not connected")
        try:
            smbclient.mkdir(self.path(dpath), **self.client_params)
        except SMBException as error:
            raise IOError(f"mkdir: {error}")

    def rmdir(self, dpath):
        self.check_connected("rmdir")
        try:
            smbclient.rmdir(self.path(dpath), **self.client_params)
        except SMBException as error:
            raise IOError(f"rmdir: {error}")

    def unlink(self, fpath):
        self.check_connected("unlink")
        try:
            smbclient.remove(self.path(fpath), **self.client_params)
        except SMBException as error:
            raise IOError(f"unlink: {error}")

    def _read_write_fd(self, fd_from, fd_to):
        while True:
            data = fd_from.read(rw_chunk_size)
            if not data:
                break
            n = 0
            while n < len(data):
                n += fd_to.write(data[n:])

    def write(self, fpath, writeobj):
        self.check_connected("write")
        try:
            with smbclient.open_file(
                self.path(fpath), mode="wb", **self.client_params
            ) as fd:
                self._read_write_fd(writeobj, fd)
        except SMBException as error:
            raise IOError(f"write: {error}")

    def read(self, fpath, readobj):
        self.check_connected("read")
        try:
            with smbclient.open_file(
                self.path(fpath), mode="rb", **self.client_params
            ) as fd:
                self._read_write_fd(fd, readobj)
        except SMBException as error:
            raise IOError(f"write: {error}")

    def write_text(self, fpath, teststr):
        self.check_connected("write_text")
        try:
            with smbclient.open_file(
                self.path(fpath), mode="w", **self.client_params
            ) as fd:
                fd.write(teststr)
        except SMBException as error:
            raise IOError(f"write: {error}")

    def read_text(self, fpath):
        self.check_connected("read_text")
        try:
            with smbclient.open_file(self.path(fpath), **self.client_params) as fd:
                ret = fd.read()
        except SMBException as error:
            raise IOError(f"write: {error}")
        return ret
