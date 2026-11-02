#!/usr/bin/env python3
"""NFSv4.1 OPEN with explicit share_access / share_deny (+ optional LOCK).

Modes:
  --once   OPEN (+ optional --lock), print RESULT=, CLOSE, exit
  --hold   OPEN (+ optional --lock), SEQUENCE renew until --stop-file appears

With --want-deleg, CREATE_SESSION requests CONN_BACK_CHAN. While holding,
inbound CB_RECALL is ACKed and DELEGRETURN is issued on the fore channel
so conflicting peer opens (WO/RW) can complete.

Exit codes: 0=NFS4_OK, 2=NFS4ERR_GRACE, 3=NFS4ERR_SHARE_DENIED, 1=other
"""
from __future__ import print_function

import argparse
import os
import select
import socket
import struct
import sys
import time
import uuid

RPC_CALL, RPC_REPLY = 0, 1
AUTH_NULL, AUTH_SYS = 0, 1
NFS4_PROGRAM, NFS_V4, NFSPROC4_COMPOUND = 100003, 4, 1

# Callback program (transient); negotiated in CREATE_SESSION
CB_PROGRAM = 0x40000000
CB_V1 = 1
CB_NULL, CB_COMPOUND = 0, 1

OP_CLOSE = 4
OP_DELEGRETURN = 8
OP_GETFH = 10
OP_LOCK = 12
OP_LOCKU = 14
OP_LOOKUP = 15
OP_OPEN = 18
OP_PUTFH = 22
OP_PUTROOTFH = 24
OP_EXCHANGE_ID = 42
OP_CREATE_SESSION = 43
OP_DESTROY_SESSION = 44
OP_SEQUENCE = 53
OP_DESTROY_CLIENTID = 57
OP_RECLAIM_COMPLETE = 58

OP_CB_RECALL = 4
OP_CB_SEQUENCE = 11

NFS4_OK = 0
NFS4ERR_DENIED = 10010
NFS4ERR_EXPIRED = 10011
NFS4ERR_DELAY = 10008
NFS4ERR_GRACE = 10013
NFS4ERR_SHARE_DENIED = 10015
NFS4ERR_NO_GRACE = 10027
NFS4ERR_BADSESSION = 10052
NFS4ERR_DEADSESSION = 10078

OPEN4_NOCREATE, OPEN4_CREATE = 0, 1
UNCHECKED4 = 0
CLAIM_NULL = 0
SP4_NONE = 0
OPEN4_SHARE_ACCESS_WANT_READ_DELEG = 0x00000100
OPEN4_SHARE_ACCESS_WANT_WRITE_DELEG = 0x00000200
OPEN4_SHARE_ACCESS_WANT_ANY_DELEG = 0x00000300
OPEN4_SHARE_ACCESS_WANT_NO_DELEG = 0x00000400
OPEN_DELEGATE_NONE = 0
OPEN_DELEGATE_READ = 1
OPEN_DELEGATE_WRITE = 2

CREATE_SESSION4_FLAG_CONN_BACK_CHAN = 0x00000002

NFS4_READ_LT = 1
NFS4_WRITE_LT = 2

ACCESS = {"r": 1, "w": 2, "rw": 3}
DENY = {"n": 0, "r": 1, "w": 2, "b": 3}
LOCK_KIND = {"nrlock": NFS4_READ_LT, "nwlock": NFS4_WRITE_LT}

OPNAME = {
    OP_CLOSE: "CLOSE",
    OP_DELEGRETURN: "DELEGRETURN",
    OP_GETFH: "GETFH",
    OP_LOCK: "LOCK",
    OP_LOCKU: "LOCKU",
    OP_LOOKUP: "LOOKUP",
    OP_OPEN: "OPEN",
    OP_PUTFH: "PUTFH",
    OP_PUTROOTFH: "PUTROOTFH",
    OP_EXCHANGE_ID: "EXCHANGE_ID",
    OP_CREATE_SESSION: "CREATE_SESSION",
    OP_DESTROY_SESSION: "DESTROY_SESSION",
    OP_SEQUENCE: "SEQUENCE",
    OP_DESTROY_CLIENTID: "DESTROY_CLIENTID",
    OP_RECLAIM_COMPLETE: "RECLAIM_COMPLETE",
}

ERRNAME = {
    NFS4_OK: "NFS4_OK",
    NFS4ERR_DELAY: "NFS4ERR_DELAY",
    NFS4ERR_DENIED: "NFS4ERR_DENIED",
    NFS4ERR_EXPIRED: "NFS4ERR_EXPIRED",
    NFS4ERR_GRACE: "NFS4ERR_GRACE",
    NFS4ERR_SHARE_DENIED: "NFS4ERR_SHARE_DENIED",
    NFS4ERR_NO_GRACE: "NFS4ERR_NO_GRACE",
    NFS4ERR_BADSESSION: "NFS4ERR_BADSESSION",
    NFS4ERR_DEADSESSION: "NFS4ERR_DEADSESSION",
}


def pad(n):
    return (4 - (n % 4)) % 4


def pack_opaque(data):
    data = data if isinstance(data, (bytes, bytearray)) else data.encode()
    return struct.pack(">I", len(data)) + data + b"\x00" * pad(len(data))


def pack_string(s):
    return pack_opaque(s.encode() if isinstance(s, str) else s)


def emit_result(kind, status):
    name = ERRNAME.get(status, str(status))
    print("%s=%s" % (kind, name))
    sys.stdout.flush()
    return name


class Unpack(object):
    def __init__(self, buf):
        self.buf = buf
        self.off = 0

    def u32(self):
        v = struct.unpack_from(">I", self.buf, self.off)[0]
        self.off += 4
        return v

    def u64(self):
        v = struct.unpack_from(">Q", self.buf, self.off)[0]
        self.off += 8
        return v

    def opaque(self):
        n = self.u32()
        data = self.buf[self.off : self.off + n]
        self.off += n + pad(n)
        return data

    def skip(self, n):
        self.off += n

    def remaining(self):
        return len(self.buf) - self.off


class Nfs41(object):
    def __init__(self, host, port, uid, gid):
        self.uid = uid
        self.gid = gid
        self.xid = int(time.time()) & 0x7FFFFFFF
        self.sock = socket.create_connection((host, port), timeout=30)
        self.sock.setblocking(True)
        self.clientid = None
        self.eir_seq = 0
        self.sessionid = None
        self.seqid = 0
        self.verifier = os.urandom(8)
        self.owner = b"open-share-" + uuid.uuid4().bytes
        self.lock_owner = b"lk-" + self.owner[:16]
        self.lock_seqid = 0
        self.open_seqid = 0
        self.cb_program = CB_PROGRAM
        self.backchannel = False
        self.deleg_stateid = None
        self.hold_fh = None
        self._pending_delegreturn = None  # (fh, stateid) after CB_RECALL ACK
        self._in_rpc = False

    def close_sock(self):
        try:
            self.sock.close()
        except Exception:
            pass

    def _auth_sys(self):
        stamp = int(time.time()) & 0xFFFFFFFF
        host = socket.gethostname().encode()[:255]
        body = (
            struct.pack(">I", stamp)
            + pack_opaque(host)
            + struct.pack(">III", self.uid, self.gid, 1)
            + struct.pack(">I", self.gid)
        )
        return struct.pack(">II", AUTH_SYS, len(body)) + body

    def _recv_fragment(self):
        chunks = []
        last = False
        while not last:
            hdr = b""
            while len(hdr) < 4:
                n = self.sock.recv(4 - len(hdr))
                if not n:
                    raise IOError("EOF reading RPC fragment")
                hdr += n
            rec = struct.unpack(">I", hdr)[0]
            last = bool(rec & 0x80000000)
            need = rec & 0x7FFFFFFF
            data = b""
            while len(data) < need:
                n = self.sock.recv(need - len(data))
                if not n:
                    raise IOError("EOF reading RPC body")
                data += n
            chunks.append(data)
        return b"".join(chunks)

    def _send_record(self, payload):
        self.sock.sendall(struct.pack(">I", 0x80000000 | len(payload)) + payload)

    def _handle_cb_call(self, raw):
        """Process inbound RPC CALL on backchannel (same TCP). Return after ACK."""
        u = Unpack(raw)
        xid = u.u32()
        mtype = u.u32()
        if mtype != RPC_CALL:
            return
        u.u32()  # rpcvers
        prog = u.u32()
        u.u32()  # vers
        proc = u.u32()
        u.u32()  # flavor
        clen = u.u32()
        u.skip(clen + pad(clen))
        u.u32()  # vflavor
        vlen = u.u32()
        u.skip(vlen + pad(vlen))

        def _reply_accept(accept_stat, body=b""):
            reply = (
                struct.pack(">IIII", xid, RPC_REPLY, 0, AUTH_NULL)
                + struct.pack(">II", 0, accept_stat)
                + body
            )
            self._send_record(reply)

        if prog != self.cb_program:
            _reply_accept(1)  # PROG_UNAVAIL
            return

        if proc == CB_NULL:
            _reply_accept(0)
            print("CB_NULL ok")
            sys.stdout.flush()
            return

        if proc != CB_COMPOUND:
            _reply_accept(2)  # PROC_UNAVAIL
            return

        u.opaque()  # tag
        u.u32()  # minor
        u.u32()  # cb_ident
        nops = u.u32()
        recall_stateid = None
        recall_fh = None
        resops = []
        cstatus = NFS4_OK

        for _ in range(nops):
            opcode = u.u32()
            if opcode == OP_CB_SEQUENCE:
                sid = u.buf[u.off : u.off + 16]
                u.skip(16)
                seq = u.u32()
                slot = u.u32()
                high = u.u32()
                u.u32()  # cache
                nref = u.u32()
                for _r in range(nref):
                    u.skip(16)
                    ncalls = u.u32()
                    u.skip(ncalls * 8)
                resops.append(
                    struct.pack(">II", OP_CB_SEQUENCE, NFS4_OK)
                    + sid
                    + struct.pack(">IIII", seq, slot, high, high)
                )
            elif opcode == OP_CB_RECALL:
                recall_stateid = u.buf[u.off : u.off + 16]
                u.skip(16)
                u.u32()  # trunc
                recall_fh = u.opaque()
                resops.append(struct.pack(">II", OP_CB_RECALL, NFS4_OK))
                print(
                    "CB_RECALL stateid=%s fh_len=%s"
                    % (recall_stateid.hex(), len(recall_fh))
                )
                sys.stdout.flush()
            else:
                cstatus = 10044  # NFS4ERR_OP_ILLEGAL
                resops.append(struct.pack(">II", opcode, cstatus))
                break

        body = (
            struct.pack(">I", cstatus)
            + pack_opaque(b"")
            + struct.pack(">I", len(resops))
            + b"".join(resops)
        )
        _reply_accept(0, body)

        # Defer DELEGRETURN until after CB reply is on the wire (and outside
        # nested demux) so the fore-channel COMPOUND is not started mid-ACK.
        if recall_stateid and recall_fh is not None:
            self._pending_delegreturn = (recall_fh, recall_stateid)

    def _flush_pending_delegreturn(self):
        pending = self._pending_delegreturn
        if not pending:
            return
        self._pending_delegreturn = None
        fh, stateid = pending
        try:
            self.do_delegreturn(fh, stateid)
        except Exception as e:
            print("DELEGRETURN after CB_RECALL failed: %s" % e)
            sys.stdout.flush()

    def _rpc(self, payload):
        """Send NFS COMPOUND CALL; demux interleaved CB CALLs on same TCP."""
        self.xid = (self.xid + 1) & 0xFFFFFFFF
        want = self.xid
        call = (
            struct.pack(
                ">IIIIII",
                self.xid,
                RPC_CALL,
                2,
                NFS4_PROGRAM,
                NFS_V4,
                NFSPROC4_COMPOUND,
            )
            + self._auth_sys()
            + struct.pack(">II", AUTH_NULL, 0)
            + payload
        )
        self._send_record(call)
        nested = self._in_rpc
        self._in_rpc = True
        try:
            while True:
                raw = self._recv_fragment()
                if len(raw) < 8:
                    raise IOError("short RPC message")
                xid = struct.unpack_from(">I", raw, 0)[0]
                mtype = struct.unpack_from(">I", raw, 4)[0]
                if mtype == RPC_CALL:
                    self._handle_cb_call(raw)
                    continue
                if mtype != RPC_REPLY:
                    continue
                if xid != want:
                    # Orphan reply — ignore
                    continue
                u = Unpack(raw)
                u.u32()  # xid
                u.u32()  # mtype
                stat = u.u32()
                if stat != 0:
                    raise RuntimeError("RPC denied stat=%s" % stat)
                u.u32()  # verf flavor
                vlen = u.u32()
                u.skip(vlen + pad(vlen))
                accept = u.u32()
                if accept != 0:
                    raise RuntimeError("RPC accept_stat=%s" % accept)
                return Unpack(raw[u.off :])
        finally:
            self._in_rpc = nested
            # Only outermost demux flushes recall→DELEGRETURN
            if not nested:
                self._flush_pending_delegreturn()

    def poll_callbacks(self, timeout=0.5):
        """Wait briefly for inbound CB; return True if something was handled."""
        r, _, _ = select.select([self.sock], [], [], timeout)
        if not r:
            return False
        raw = self._recv_fragment()
        if len(raw) < 8:
            return False
        mtype = struct.unpack_from(">I", raw, 4)[0]
        if mtype == RPC_CALL:
            self._handle_cb_call(raw)
            self._flush_pending_delegreturn()
            return True
        return False

    def _skip_sequence_ok(self, u):
        u.skip(16)
        u.u32()
        u.u32()
        u.u32()
        u.u32()
        u.u32()

    def _skip_nfsace(self, u):
        u.u32()
        u.u32()
        u.u32()
        u.opaque()

    def _skip_open_ok(self, u):
        """Return (open_stateid, deleg_type, deleg_stateid|None)."""
        stateid = u.buf[u.off : u.off + 16]
        u.skip(16)
        u.u32()
        u.u64()
        u.u64()
        u.u32()
        bmlen = u.u32()
        for _ in range(bmlen):
            u.u32()
        dtype = u.u32()
        deleg_stateid = None
        if dtype == OPEN_DELEGATE_READ:
            deleg_stateid = u.buf[u.off : u.off + 16]
            u.skip(16)
            u.u32()
            self._skip_nfsace(u)
        elif dtype == OPEN_DELEGATE_WRITE:
            deleg_stateid = u.buf[u.off : u.off + 16]
            u.skip(16)
            u.u32()
            limitby = u.u32()
            if limitby == 1:
                u.u64()
            else:
                u.u32()
                u.u32()
            self._skip_nfsace(u)
        elif dtype == 3:
            why = u.u32()
            if why in (1, 2):
                u.u32()
        return stateid, dtype, deleg_stateid

    def compound(self, ops, minor=1, with_seq=True):
        body = b""
        nops = 0
        if with_seq:
            if not self.sessionid:
                raise RuntimeError("no session")
            self.seqid += 1
            body += struct.pack(">I", OP_SEQUENCE) + self.sessionid
            body += struct.pack(">III", self.seqid, 0, 0) + struct.pack(">I", 0)
            nops += 1
        for op in ops:
            body += op
            nops += 1
        payload = pack_string("") + struct.pack(">II", minor, nops) + body
        results = []
        cstatus = -1
        for _attempt in range(8):
            u = self._rpc(payload)
            cstatus = u.u32()
            u.opaque()
            nres = u.u32()
            results = []
            delay = False
            for _ in range(nres):
                opcode = u.u32()
                status = u.u32()
                extra = None
                if status == NFS4_OK:
                    if opcode == OP_SEQUENCE:
                        self._skip_sequence_ok(u)
                    elif opcode == OP_GETFH:
                        extra = u.opaque()
                    elif opcode == OP_OPEN:
                        extra = self._skip_open_ok(u)
                    elif opcode == OP_CLOSE:
                        u.skip(16)
                    elif opcode == OP_DELEGRETURN:
                        pass
                    elif opcode == OP_LOCK:
                        extra = u.buf[u.off : u.off + 16]
                        u.skip(16)
                    elif opcode == OP_LOCKU:
                        u.skip(16)
                elif status == NFS4ERR_DENIED and opcode == OP_LOCK:
                    u.u64()
                    u.u64()
                    u.u32()
                    u.opaque()
                results.append((opcode, status, u, extra))
                if status == NFS4ERR_DELAY:
                    delay = True
                    break
                if status != NFS4_OK:
                    break
            if delay:
                time.sleep(0.5)
                continue
            return cstatus, results
        return cstatus, results

    def check(self, results, what):
        for opcode, status, _u, _extra in results:
            if status != NFS4_OK:
                err = ERRNAME.get(status, str(status))
                raise RuntimeError(
                    "%s: %s failed status=%s (%s)"
                    % (what, OPNAME.get(opcode, opcode), status, err)
                )

    def exchange_id(self):
        flags = 0x00000001 | 0x00000100 | 0x00010000
        op = (
            struct.pack(">I", OP_EXCHANGE_ID)
            + self.verifier
            + pack_opaque(self.owner)
            + struct.pack(">II", flags, SP4_NONE)
            + struct.pack(">I", 0)
        )
        _cstatus, results = self.compound([op], with_seq=False)
        self.check(results, "EXCHANGE_ID")
        u = results[0][2]
        self.clientid = u.buf[u.off : u.off + 8]
        u.skip(8)
        self.eir_seq = u.u32()
        print("EXCHANGE_ID clientid=%s seq=%s" % (self.clientid.hex(), self.eir_seq))

    def _chan_attrs(self):
        return struct.pack(
            ">IIIIII", 0, 1024 * 1024, 1024 * 1024, 2048, 16, 64
        ) + struct.pack(">I", 0)

    def create_session(self, want_backchannel=False):
        flags = CREATE_SESSION4_FLAG_CONN_BACK_CHAN if want_backchannel else 0
        # csa_cb_program + one AUTH_NONE sec_parm (required for backchannel)
        cb_tail = struct.pack(">III", self.cb_program, 1, AUTH_NULL)
        op = (
            struct.pack(">I", OP_CREATE_SESSION)
            + self.clientid
            + struct.pack(">II", self.eir_seq, flags)
            + self._chan_attrs()
            + self._chan_attrs()
            + cb_tail
        )
        _cstatus, results = self.compound([op], with_seq=False)
        self.check(results, "CREATE_SESSION")
        u = results[0][2]
        self.sessionid = u.buf[u.off : u.off + 16]
        u.skip(16)
        u.u32()  # csr_sequence
        csr_flags = u.u32()
        self.backchannel = bool(csr_flags & CREATE_SESSION4_FLAG_CONN_BACK_CHAN)
        print(
            "CREATE_SESSION sessionid=%s backchannel=%s flags=0x%x"
            % (self.sessionid.hex(), self.backchannel, csr_flags)
        )
        if want_backchannel and not self.backchannel:
            print("WARNING: server did not grant CONN_BACK_CHAN")

    def reclaim_complete(self):
        op = struct.pack(">II", OP_RECLAIM_COMPLETE, 0)
        _cstatus, results = self.compound([op])
        status = results[-1][1] if results else -1
        print("RECLAIM_COMPLETE %s" % ERRNAME.get(status, status))
        if status not in (NFS4_OK, NFS4ERR_NO_GRACE):
            print("  (continuing anyway)")

    def lookup_path(self, export, filename):
        ops = [struct.pack(">I", OP_PUTROOTFH)]
        for p in [x for x in export.strip("/").split("/") if x]:
            ops.append(struct.pack(">I", OP_LOOKUP) + pack_string(p))
        ops.append(struct.pack(">I", OP_GETFH))
        _cstatus, results = self.compound(ops)
        self.check(results, "LOOKUP export")
        dir_fh = results[-1][3]
        parent = dir_fh
        name = filename
        if "/" in filename.strip("/"):
            comps = [c for c in filename.split("/") if c]
            name = comps[-1]
            ops = [struct.pack(">I", OP_PUTFH) + pack_opaque(dir_fh)]
            for c in comps[:-1]:
                ops.append(struct.pack(">I", OP_LOOKUP) + pack_string(c))
            ops.append(struct.pack(">I", OP_GETFH))
            _cstatus, results = self.compound(ops)
            self.check(results, "LOOKUP parent")
            parent = results[-1][3]
        return parent, name

    def open_file(self, dir_fh, name, access, deny, create, want_deleg):
        sa = access
        if want_deleg:
            # Signal desire for a matching delegation (RFC 5661 OPEN want bits).
            if access == ACCESS["r"]:
                sa |= OPEN4_SHARE_ACCESS_WANT_READ_DELEG
            elif access == ACCESS["w"]:
                sa |= OPEN4_SHARE_ACCESS_WANT_WRITE_DELEG
            else:
                sa |= OPEN4_SHARE_ACCESS_WANT_ANY_DELEG
        else:
            sa |= OPEN4_SHARE_ACCESS_WANT_NO_DELEG
        self.open_seqid = (self.open_seqid + 1) & 0xFFFFFFFF
        if create:
            openhow = struct.pack(">I", OPEN4_CREATE) + struct.pack(">I", UNCHECKED4)
            openhow += struct.pack(">I", 0)
        else:
            openhow = struct.pack(">I", OPEN4_NOCREATE)
        op = (
            struct.pack(">I", OP_OPEN)
            + struct.pack(">I", self.open_seqid)
            + struct.pack(">II", sa, deny)
            + self.clientid
            + pack_opaque(b"oo-" + self.owner[:16])
            + openhow
            + struct.pack(">I", CLAIM_NULL)
            + pack_string(name)
        )
        ops = [
            struct.pack(">I", OP_PUTFH) + pack_opaque(dir_fh),
            op,
            struct.pack(">I", OP_GETFH),
        ]
        cstatus, results = self.compound(ops)
        opcode, status, _u, extra = results[-2] if len(results) >= 2 else results[-1]
        if cstatus != NFS4_OK and status == NFS4_OK:
            status = cstatus
        err = ERRNAME.get(status, str(status))
        print(
            "OPEN name=%s access=%s deny=%s -> %s (%s) compound=%s"
            % (name, access, deny, status, err, cstatus)
        )
        emit_result("RESULT", status)
        if status != NFS4_OK or cstatus != NFS4_OK:
            return None, None, status if status != NFS4_OK else cstatus
        if not extra or not isinstance(extra, tuple):
            print("  stateid=None (OPEN incomplete)")
            emit_result("RESULT", NFS4ERR_GRACE)
            return None, None, NFS4ERR_GRACE
        stateid, dtype, deleg_stateid = extra
        if not stateid:
            print("  stateid=None (OPEN incomplete)")
            emit_result("RESULT", NFS4ERR_GRACE)
            return None, None, NFS4ERR_GRACE
        print("  stateid=%s" % stateid.hex())
        print("  deleg_type=%s" % dtype)
        if deleg_stateid:
            self.deleg_stateid = deleg_stateid
            print("  deleg_stateid=%s" % deleg_stateid.hex())
        elif want_deleg:
            print("  WARNING: --want-deleg but no delegation granted")
        fh = results[-1][3]
        self.hold_fh = fh
        return stateid, fh, status

    def lock_file(self, fh, open_stateid, kind):
        locktype = LOCK_KIND[kind]
        self.lock_seqid = (self.lock_seqid + 1) & 0xFFFFFFFF
        locker = (
            struct.pack(">I", 1)
            + struct.pack(">I", self.open_seqid)
            + open_stateid
            + struct.pack(">I", self.lock_seqid)
            + pack_opaque(self.lock_owner)
        )
        op = (
            struct.pack(">I", OP_LOCK)
            + struct.pack(">I", locktype)
            + struct.pack(">I", 0)
            + struct.pack(">QQ", 0, 0xFFFFFFFFFFFFFFFF)
            + locker
        )
        ops = [struct.pack(">I", OP_PUTFH) + pack_opaque(fh), op]
        _cstatus, results = self.compound(ops)
        status = results[-1][1]
        extra = results[-1][3]
        emit_result("LOCK_RESULT", status)
        print("LOCK %s -> %s" % (kind, ERRNAME.get(status, status)))
        if status != NFS4_OK:
            return None, status
        return extra, status

    def renew(self):
        cstatus, results = self.compound([])
        status = results[0][1] if results else -1
        err = ERRNAME.get(status, str(status))
        print("SEQUENCE seq=%s -> %s compound=%s" % (self.seqid, err, cstatus))
        if status != NFS4_OK:
            raise RuntimeError("lease renew failed: %s" % err)

    def do_delegreturn(self, fh, stateid):
        op = struct.pack(">I", OP_DELEGRETURN) + stateid
        ops = [struct.pack(">I", OP_PUTFH) + pack_opaque(fh), op]
        _cstatus, results = self.compound(ops)
        status = results[-1][1]
        print("DELEGRETURN -> %s" % ERRNAME.get(status, status))
        if status == NFS4_OK:
            print("RECALL_HANDLED=OK")
            self.deleg_stateid = None
        else:
            print("RECALL_HANDLED=FAIL")
        sys.stdout.flush()
        return status

    def hold_until_stop(self, interval, stop_path):
        """Hold OPEN; poll stop-file + backchannel CB_RECALL until stop."""
        print(
            "Holding OPEN. SEQUENCE every %ss until %s (backchannel=%s)"
            % (interval, stop_path, self.backchannel)
        )
        sys.stdout.flush()
        elapsed = 0.0
        step = 0.5
        while not os.path.exists(stop_path):
            try:
                self.poll_callbacks(timeout=step)
            except Exception as e:
                print("poll_callbacks: %s" % e)
                sys.stdout.flush()
                time.sleep(step)
            elapsed += step
            if os.path.exists(stop_path):
                break
            if elapsed >= interval:
                self.renew()
                elapsed = 0.0
        print("stop-file seen, closing")
        sys.stdout.flush()

    def do_unlock(self, fh, lock_stateid, kind):
        locktype = LOCK_KIND[kind]
        self.lock_seqid = (self.lock_seqid + 1) & 0xFFFFFFFF
        op = (
            struct.pack(">I", OP_LOCKU)
            + struct.pack(">I", locktype)
            + struct.pack(">I", self.lock_seqid)
            + lock_stateid
            + struct.pack(">QQ", 0, 0xFFFFFFFFFFFFFFFF)
        )
        ops = [struct.pack(">I", OP_PUTFH) + pack_opaque(fh), op]
        _cstatus, results = self.compound(ops)
        status = results[-1][1]
        print("LOCKU %s -> %s" % (kind, ERRNAME.get(status, status)))
        sys.stdout.flush()
        return status

    def do_close(self, fh, stateid):
        # Return any remaining deleg before CLOSE
        if self.deleg_stateid:
            try:
                self.do_delegreturn(fh, self.deleg_stateid)
            except Exception as e:
                print("DELEGRETURN before CLOSE: %s" % e)
        self.open_seqid = (self.open_seqid + 1) & 0xFFFFFFFF
        op = struct.pack(">I", OP_CLOSE) + struct.pack(">I", self.open_seqid) + stateid
        ops = [struct.pack(">I", OP_PUTFH) + pack_opaque(fh), op]
        _cstatus, results = self.compound(ops)
        status = results[-1][1]
        print("CLOSE -> %s" % ERRNAME.get(status, status))
        if status == NFS4_OK:
            print("CLEAN_CLOSE=OK")
        else:
            print("CLEAN_CLOSE=FAIL")
        sys.stdout.flush()

    def teardown(self):
        try:
            if self.sessionid:
                op = struct.pack(">I", OP_DESTROY_SESSION) + self.sessionid
                self.compound([op], with_seq=False)
        except Exception as e:
            print("DESTROY_SESSION: %s" % e)
        try:
            if self.clientid:
                op = struct.pack(">I", OP_DESTROY_CLIENTID) + self.clientid
                self.compound([op], with_seq=False)
        except Exception:
            pass
        self.close_sock()


def _exit_for_status(status):
    if status == NFS4_OK:
        return 0
    if status == NFS4ERR_GRACE:
        return 2
    if status == NFS4ERR_SHARE_DENIED:
        return 3
    return 1


def main():
    p = argparse.ArgumentParser(description="NFSv4.1 OPEN with share_deny")
    p.add_argument("--server", required=True)
    p.add_argument("--port", type=int, default=2049)
    p.add_argument("--export", required=True, help="Ganesha export path")
    p.add_argument("--file", default="f0.txt")
    p.add_argument("--access", choices=ACCESS, default="rw")
    p.add_argument("--deny", choices=DENY, default="n")
    p.add_argument("--create", action="store_true")
    p.add_argument("--want-deleg", action="store_true")
    p.add_argument("--lock", choices=LOCK_KIND, default=None)
    p.add_argument(
        "--renew",
        type=float,
        default=20,
        help="SEQUENCE interval while holding",
    )
    p.add_argument("--once", action="store_true", help="OPEN (+lock), CLOSE, exit")
    p.add_argument("--hold", action="store_true", help="OPEN (+lock), renew until stop")
    p.add_argument(
        "--stop-file",
        default=None,
        help="Path that ends --hold (created by test harness)",
    )
    p.add_argument("--uid", type=int, default=os.getuid())
    p.add_argument("--gid", type=int, default=os.getgid())
    args = p.parse_args()

    if not args.once and not args.hold:
        args.once = True
    if args.hold and not args.stop_file:
        p.error("--hold requires --stop-file")

    access, deny = ACCESS[args.access], DENY[args.deny]
    print(
        "target %s:%s export=%s file=%s access=%s(%d) deny=%s(%d) lock=%s "
        "want_deleg=%s"
        % (
            args.server,
            args.port,
            args.export,
            args.file,
            args.access,
            access,
            args.deny,
            deny,
            args.lock,
            args.want_deleg,
        )
    )

    c = Nfs41(args.server, args.port, args.uid, args.gid)
    stateid = fh = None
    lock_stateid = None
    open_status = -1
    try:
        c.exchange_id()
        c.create_session(want_backchannel=args.want_deleg)
        c.reclaim_complete()
        parent, name = c.lookup_path(args.export, args.file)
        stateid, fh, open_status = c.open_file(
            parent, name, access, deny, args.create, args.want_deleg
        )
        if open_status != NFS4_OK:
            sys.exit(_exit_for_status(open_status))

        if args.lock:
            lock_stateid, lock_status = c.lock_file(fh, stateid, args.lock)
            if lock_status != NFS4_OK:
                sys.exit(_exit_for_status(lock_status))

        if args.hold:
            print("READY")
            sys.stdout.flush()
            try:
                c.hold_until_stop(args.renew, args.stop_file)
            except KeyboardInterrupt:
                print("\ninterrupted")
        else:
            print("READY")
            sys.stdout.flush()
    finally:
        if lock_stateid and fh and args.lock:
            try:
                c.do_unlock(fh, lock_stateid, args.lock)
            except Exception as e:
                print("LOCKU failed: %s" % e)
        if stateid and fh:
            try:
                c.do_close(fh, stateid)
            except Exception as e:
                print("CLOSE failed: %s" % e)
                print("CLEAN_CLOSE=FAIL")
                sys.stdout.flush()
        c.teardown()

    sys.exit(0)


if __name__ == "__main__":
    main()
