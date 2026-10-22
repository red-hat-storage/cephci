# NFS-Ganesha gRPC API Tests

gRPC **interface** automation for NFS-Ganesha on **CephFS (cephadm NFS)**.

This folder validates that Ganesha gRPC APIs work as expected. It does **not**:

- Re-run NFS protocol regression (cthon, pynfs, locks, POSIX, scale) — those
  stay in existing NFS suites
- Assert D-Bus vs gRPC parity — D-Bus is still the cephadm management path
- Cover cephadm switching to gRPC — that is a later release

## Scope

**FSAL:** CephFS only (`ceph nfs cluster`, cephadm NFS service).

| In scope | Out of scope (for now) |
|----------|------------------------|
| ExportService, ExportStats | GPFS / IBM Storage Scale (`nfs_upstream`) |
| ClientMgr, ClientStats | D-Bus parity / golden compare |
| nfsAdmin, ShowIdMapper | Inter-node HA / grace inject |
| Core nfsService (grace, client/session IDs) | LogService (no upstream proto yet) |
| Transport: TCP, UDS, mTLS, negatives | Cephadm consuming gRPC for export mgmt |

## Suites

| Suite | Purpose |
|-------|---------|
| `suites/tentacle/nfs/tier1-nfs-ganesha-grpc.yaml` | Core `nfsService` smoke (port, discovery, grace, IDs) |
| `suites/tentacle/nfs/tier1-nfs-ganesha-grpc-admin.yaml` | Full gRPC API suite (export/client/admin/stats + security) |
| `suites/tentacle/nfs/tier1-nfs-ganesha-grpc-perf.yaml` | RPC latency + image build / proto compile |

**Removed:** `tier0-nfs-ganesha-grpc-regression.yaml` (cthon/pynfs/locks/POSIX/tier gates/scale).

## Related artifacts

- **Proto definitions:** https://github.com/nfs-ganesha/nfs-ganesha/tree/next/src/grpc_server/proto
- **Polarion mapping:** [`POLARION.md`](POLARION.md)

## Module map

| File | Purpose |
|------|---------|
| `grpc_client.py` | grpcurl helpers for all proto services |
| `grpc_deploy.py` | NFS container image override + optional cert copy |
| `test_grpc_ganesha_admin.py` | gRPC API operations (`operation` config key) |
| `test_grpc_ganesha_security_negative.py` | Transport / mTLS / invalid RPC / timeout |
| `test_grpc_ganesha_load_negative.py` | Concurrent grpcurl load |
| `test_grpc_ganesha_perf.py` | Admin / ShowClients / export RPC latency |
| `test_grpc_ganesha_build.py` | USE_GRPC image + proto compile smoke |
| `test_grpc_ganesha_deploy.py` | Image override + bootstrap smoke |
| `test_grpc_ganesha_parity.py` | Deferred D-Bus parity (not scheduled) |
| `test_grpc_ganesha_stub.py` | Parked HA Polarion placeholders (not scheduled) |
| `test_grpc_ganesha_regression_gate.py` | Legacy tier gates (not scheduled) |
| `test_grpc_pjdfstest.py` | POSIX smoke (not scheduled in gRPC suites) |

## CephCI module naming

```
module: grpc_admin.test_grpc_ganesha_admin.py
```

`tests/nfs` is on `sys.path`; the folder uses an underscore (`grpc_admin`) because
hyphens are not valid in Python import paths.

## Upstream gRPC services (nfs-ganesha `next`)

| Package | Service | Key RPCs |
|---------|---------|----------|
| `nfsService` | `GetClientId` | `GetClientIds` |
| `nfsService` | `GetNfsGrace` | `GetGracePeriod` |
| `nfsService` | `StartNfsGrace` | `StartGraceWithEvent` |
| `nfsService` | `GetSessionId` | `GetSessionIds` |
| `nfsService` | `nfsAdmin` | `ShutdownGanesha`, `ReReadConfig`, Purge*, Trim*, … |
| `nfsService` | `ShowIdMapperService` | `ShowIdMapper` |
| `exportService` | `ExportService` | `ShowExports`, `DisplayExport`, `AddExport`, `RemoveExport`, `UpdateExport` |
| `exportService` | `ExportStats` | `GetNFSv*IO`, `GetNFSIO`, `StatusStats`, `ResetStats`, … |
| `cltmgrService` | `ClientMgr` | `ShowClients`, `AddClient`, `RemoveClient`, `DisconnectNfsv41Client` |
| `cltmgrService` | `ClientStats` | `GetNFSv*IO`, `GetDelegations`, `GetClientAllops`, … |

**Not in proto yet:** Log get/set (D-Bus log interface only).

## Admin module operations

| `operation` | gRPC call |
|-------------|-----------|
| `verify_port` | Port 50051 listen check |
| `list_services` | `grpcurl list` |
| `get_grace_period` | `GetNfsGrace/GetGracePeriod` |
| `start_grace_event_0\|2\|4\|5` | `StartNfsGrace/StartGraceWithEvent` |
| `show_clients_and_sessions` | `GetClientIds` + `GetSessionIds` |
| `show_exports` | `ExportService/ShowExports` |
| `display_export` | `ExportService/DisplayExport` |
| `add_export` | `ExportService/AddExport` |
| `remove_export` | `ExportService/RemoveExport` |
| `update_export` | `ExportService/UpdateExport` (flips Access_Type) |
| `show_clients_mgr` | `ClientMgr/ShowClients` |
| `add_client` / `remove_client` | `ClientMgr/AddClient` / `RemoveClient` |
| `disconnect_nfsv41_client` | `ClientMgr/DisconnectNfsv41Client` |
| `reread_config` | `nfsAdmin/ReReadConfig` |
| `purge_gids` | `nfsAdmin/PurgeGids` |
| `purge_netgroups` | `nfsAdmin/PurgeNetGroups` |
| `init_fd_limit` | `nfsAdmin/InitFdLimit` |
| `purge_idmapper_cache` | `nfsAdmin/PurgeIdmapperCache` |
| `purge_idmapper_negative_cache` | `nfsAdmin/PurgeIdmapperNegativeCache` |
| `malloc_trace` / `malloc_untrace` | `nfsAdmin/MallocTrace` / `MallocUntrace` |
| `trim_enable` / `trim_disable` | `nfsAdmin/TrimEnableDisable` |
| `trim_call` | `nfsAdmin/TrimCall` |
| `trim_status` | `nfsAdmin/TrimStatus` |
| `show_idmapper` | `ShowIdMapperService/ShowIdMapper` |
| `export_stats_smoke` | `ExportStats` smoke (StatusStats / GetNFSv41IO / GetNFSIO) |
| `client_stats_smoke` | `ClientStats.GetNFSv41IO` smoke |
| `shutdown` | `nfsAdmin/ShutdownGanesha` — **not scheduled** (opt-in only) |

Blocked (no upstream proto): `set_log_level`, `get_log_level`.

**Deferred:** full ExportStats / ClientStats RPC matrix (beyond smoke).

## gRPC configuration defaults

| Parameter | Default |
|-----------|---------|
| GRPC_ENABLE | true |
| GRPC_PORT | 50051 |
| Server cert | `/etc/ganesha/certs/server.crt` |
| Server key | `/etc/ganesha/certs/server.key` |
| Client cert | `/etc/ganesha/certs/client.crt` |
| Client key | `/etc/ganesha/certs/client.key` |
| CA cert | `/etc/ganesha/certs/ca.crt` |

Build flag: `cmake -DUSE_GRPC=ON`

## grpcurl transport (CephCI)

Suites call `setup_grpc_transport()` in `grpc_client.py` (same rules as
`tests/nfs/test_nfs_grpc.py`):

- **Ceph >= 20.2.2:** mTLS — copy `ca.crt`, `client.crt`, `client.key` from the
  admin node
  `/var/lib/ceph/<fsid>/nfs_grpc-client-certs/nfs.<nfs_name>/` to each grpcurl
  client under `/root/nfs_grpc_certs`, then use `-cacert/-cert/-key`.
- **Older Ceph:** `-plaintext`.

Overrides: `force_plaintext: true`, or `use_tls: true|false`.

Example grpcurl (mTLS on 20.2.2+):

```
grpcurl -cacert /root/nfs_grpc_certs/ca.crt \
  -cert /root/nfs_grpc_certs/client.crt \
  -key /root/nfs_grpc_certs/client.key \
  <nfs-ip>:50051 list
```

## gRPC-enabled cephadm NFS image

All gRPC suites expect a **gRPC-built** nfs-ganesha container:

```yaml
config:
  nfs_container_image: quay.io/rhceph/nfs-ganesha-grpc:<tag>
```

`test_grpc_ganesha_deploy.py` operation `set_nfs_image` applies
`mgr/cephadm/container_image_nfs` via `grpc_deploy.apply_nfs_container_image()`.

## Optional manual server cert deploy (legacy / dev)

When testing custom server TLS material (not mgr `nfs_grpc-client-certs`),
place files on the **NFS node** and enable:

```
/root/certs/ca.crt
/root/certs/server.crt
/root/certs/server.key
/root/certs/client.crt
/root/certs/client.key
```

```yaml
config:
  deploy_grpc_certs: true
  grpc_cert_source: /root/certs
  restart_nfs_after_cert_copy: true
```

## Later release (not in these suites)

- Cephadm manages exports/clients via gRPC instead of D-Bus
- D-Bus removal / dual-write parity gates
- HA / multi-node gRPC coordination
- LogService once upstream lands a proto

## Open items

- [ ] Published gRPC NFS container image tag for Jenkins
- [ ] Harden `AddExport` / `UpdateExport` with real CEPH FSAL credentials from the live export
  (configs are staged under the bind-mounted ``etc/ganesha`` path)
- [ ] Optional `require_all_services: true` once the full-proto image is standard
- [ ] Full ExportStats / ClientStats matrix (deferred)
- [ ] `ShutdownGanesha` isolated job (intentionally skipped)
