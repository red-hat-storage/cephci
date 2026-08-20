# CephFS Idmapped Mount Tests

Standalone functional tests for CephFS **idmapped kernel mounts** on bare Ceph clusters.

| Item | Value |
|------|-------|
| Related RFE | RHSTOR-8028 / ODFRFE-119 |
| Ceph release | Tentacle |
| Client OS | RHEL 10.2 |
| Mount type | Kernel (`mount -t ceph`) only |
| Out of scope | ODF, Kubernetes, CSI, `hostUsers: false` pod lifecycle |

## Overview

These tests validate that the Linux kernel CephFS client accepts idmapped bind mounts
(`mount_setattr` / `X-mount.idmap`) and that UID/GID translation, isolation, and POSIX
operations behave correctly. They do **not** require OpenShift Data Foundation or the
CephFS CSI driver.

## Test Cases

| Module | TC | Description | Priority |
|--------|-----|-------------|----------|
| `test_idmap_preflight.py` | TC-S0 | Cluster health, kernel CephFS module, plain mount smoke | P0 |
| `test_idmap_user_ns_baseline.py` | TC-S1 | User namespace mapping on client OS | P0 |
| `test_idmap_mount_creation.py` | TC-S2 | Idmapped bind mount creation (**blocker**) | P0 |
| `test_idmap_ownership.py` | TC-S3 | UID/GID translation correctness | P0 |
| `test_idmap_isolation.py` | TC-S4 | Security isolation between different maps | P0 |
| `test_idmap_plain_regression.py` | TC-S5 | Plain (non-idmapped) mount regression | P0 |
| `test_idmap_multi_client.py` | TC-S6 | Multi-client RWX sharing (requires 2 clients) | P0 |
| `test_idmap_posix.py` | TC-S7 | chmod, chown, chgrp, directory CRUD | P1 |
| `test_idmap_recursive_tools.py` | TC-S8 | cp, tar, rsync, rm -rf | P1 |
| `test_idmap_remount.py` | TC-S9 | Remount / client recovery | P1 |
| `test_idmap_negative_auth.py` | TC-S10 | Bad cephx credentials | P1 |
| `test_idmap_negative_fsname.py` | TC-S11 | Invalid filesystem name | P1 |
| `test_idmap_xfstests.py` | TC-S13 | xfstests `idmapped` group | P1 |

**Go/No-Go:** TC-S2, TC-S3, TC-S4, and TC-S6 must all pass.

## Directory Layout

```
tests/cephfs/cephfs_idmap/
├── README.md
├── lib/
│   └── cephfs_idmap_lib.py      # Shared mount, unshare, and assertion helpers
├── test_idmap_preflight.py
├── test_idmap_user_ns_baseline.py
├── test_idmap_mount_creation.py
├── test_idmap_ownership.py
├── test_idmap_isolation.py
├── test_idmap_plain_regression.py
├── test_idmap_multi_client.py
├── test_idmap_posix.py
├── test_idmap_recursive_tools.py
├── test_idmap_remount.py
├── test_idmap_negative_auth.py
├── test_idmap_negative_fsname.py
└── test_idmap_xfstests.py
```

## Suite

Run the full suite:

```bash
python run.py --rhbuild <tentacle-build> --platform rhel-10 \
  --suite suites/tentacle/cephfs/tier-2_cephfs_test-idmap.yaml \
  --global-conf conf/tentacle/cephfs/tier-2_cephfs_7-node-cluster.yaml \
  --cloud openstack \
  --inventory conf/inventory/ibm-rhel-10.2-server.yaml
```

## Prerequisites

- Standalone Ceph cluster (Tentacle) with `cephfs` volume and active MDS
- At least **2 client nodes** for TC-S6 (`node8`, `node9` in default conf)
- RHEL 10.2 clients with `ceph-common` and kernel CephFS client
- `util-linux` with `X-mount.idmap` support (included in RHEL 10.2)

**Note:** Cluster conf may define four client-role nodes (`node8`–`node11`), but the
suite only configures two via `test_client.py`. Idmap tests use the first two
configured clients only; unconfigured client VMs are ignored.

## Shared Library

`lib/cephfs_idmap_lib.py` provides:

- `IdmapTestHelper` — mount/unmount, idmap bind, ownership assertions, dmesg checks
- `init_idmap_test()` — standard test initialization
- Default idmap: `b:100000:0:65536` (on-disk UID 100000–165535 → view UID 0–65535)

## Execution Order

Tests are ordered in the suite YAML to match the standalone test plan:

1. TC-S0 (gate) → TC-S1 (baseline) → TC-S2 (blocker)
2. TC-S3, TC-S4, TC-S5, TC-S6 (P0 functional)
3. TC-S7–TC-S11, TC-S13 (P1)

## References

- [RHSTOR-8028 / ODFRFE-119](https://bugzilla.redhat.com/) — User Namespace & CephFS Compatibility
- Upstream CephFS idmap: kernel ~6.7+, MDS `CEPHFS_FEATURE_HAS_OWNER_UIDGID`
- xfstests: `sudo ./check -g idmapped` with `FSTYP=ceph`
