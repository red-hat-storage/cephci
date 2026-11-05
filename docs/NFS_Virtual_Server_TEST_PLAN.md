# Regional File Mount / NFS Virtual Server — QE Strategy & Test Plan

> **Feature:** [ISCE-1982](https://jsw.ibm.com/browse/ISCE-1982) — one NFS server on multiple networks; each server NIC looks like a different server to clients on that network  
> **RFE:** [IBMCEPH-11530](https://ibm-ceph.atlassian.net/browse/IBMCEPH-11530) — disable virtual server after it was enabled, then turn it back on, without deleting the NFS cluster  
> **Linked BZ:** [2370079](https://bugzilla.redhat.com/show_bug.cgi?id=2370079)  
> **Scope:** cephadm-managed NFS, CephFS export, dual-homed NFS node and clients  
> **CephCI suite:** `suites/tentacle/nfs/tier-1-nfs-virtual-server-multi-share-export.yaml`  
> **Conf:** `conf/tentacle/nfs/virtual-server-multi-subnet.yaml`

---

## 1. Executive Summary

Regional File Mount lets one NFS-Ganesha daemon serve clients that sit on different networks, as long as the NFS node itself is attached to those networks. To the client, each server address behaves like its own NFS server. Under the hood it is still one cluster and one Ganesha.

The cluster is created with:

```bash
ceph nfs cluster create <cluster_id> <placement> --enable-virtual-server
```

That stores `enable_virtual_server: true` on the NFS service spec and renders:

```text
Virtual_Server = true;
```

in the live `ganesha.conf`.

| Behavior | Expected |
|---|---|
| Same export, different server/client address pairs | Mount and IO succeed on every reachable route |
| One export, several clients | Each client mounts via its own route |
| Several exports | Each client can use a distinct route |
| Export fencing (`Server_Addrs` + `CLIENT`) | Only the listed server IP and client IP can mount that export |
| Other exports on the same cluster | Stay multi-route; fencing on one export does not fence the rest |
| Disable after enable ([IBMCEPH-11530](https://ibm-ceph.atlassian.net/browse/IBMCEPH-11530)) | Spec flag is no longer true; `Virtual_Server = false;`; cluster and exports remain |
| Re-enable | `Virtual_Server = true;` again; multi-route mount still works |

**Go/No-Go for the feature:** dual-homed nodes → `--enable-virtual-server` → `Virtual_Server = true` → mount and IO on every route.

**Go/No-Go for the RFE:** same cluster, orch apply `enable_virtual_server: false`, daemon reconfig → conf is false, export still mounts → apply `true` again → conf is true and a second route still mounts.

Disabling virtual server does **not** remove the extra NICs. A mount to another server address can still succeed. Route failure after a spec change is the fencing cases (TC-VS6 / TC-VS7), not the disable case.

---

## 2. QE Strategy

### 2.1 What we are validating

Three layers:

1. **API / spec surface** — `ceph nfs cluster create --enable-virtual-server` is accepted and the orch spec keeps `enable_virtual_server`.
2. **Config generation** — container `ganesha.conf` contains `Virtual_Server = true;` or `Virtual_Server = false;` after reconfig.
3. **Runtime behavior** — clients on each shared network can mount and run IO; fencing restricts one export; disable/re-enable does not delete the cluster.

### 2.2 Delivery shape

| Layer | What exists today |
|---|---|
| CLI | `ceph nfs cluster create ... --enable-virtual-server` |
| Service spec | `spec.enable_virtual_server: true` after create |
| Disable / re-enable | No dedicated CLI flag. Export the service, edit the bool, `ceph orch apply -i`, then `ceph orch daemon reconfig <nfs daemon>` |
| Ganesha | `Virtual_Server = true\|false;` in the managed conf |
| CephCI | Scenarios 1–5 in the virtual-server suite. Scenario 6 covers the RFE toggle |

Confirm the exact conf block against the build under test. Do not assume a second CLI such as `ceph nfs cluster set`.

### 2.3 Version / build gates

| Component | Requirement |
|---|---|
| Ceph | Build whose `ceph nfs cluster create --help` contains `--enable-virtual-server` |
| NFS-Ganesha image | Renders and honors `Virtual_Server` |
| RFE | Build where an existing cluster accepts `enable_virtual_server: false` without cluster delete. ON_QA target discussed as 9.2 when 10.0 builds are unavailable |
| Lab | NFS node and each client have 2+ IPv4 addresses on matching provider networks |

Record Ceph version, Ganesha package or image digest, and `ip -o -4 addr show` on the NFS node and clients.

### 2.4 Risk-based priorities

| Risk | Why | Priority |
|---|---|---|
| `--enable-virtual-server` accepted but `Virtual_Server` never rendered | Silent no-op | **P0** |
| Mount works on one route and fails on the second | Feature miss; clients on the other network cannot use the export | **P0** |
| Disable deletes the cluster or the export | RFE regression; operator loses the share | **P0** |
| Spec says false but conf stays `Virtual_Server = true` | Reconfig did not apply | **P0** |
| Re-enable leaves conf false, or the second route stops mounting | Toggle is one-way | **P0** |
| Fencing blocks only one pair and leaves other routes open | [IBMCEPH-13896](https://ibm-ceph.atlassian.net/browse/IBMCEPH-13896) | **P0** (known fail) |
| Route picker pairs the wrong client NIC (`/24` guess) | Automation false fail; product may be fine | **P1** |
| Two NFS daemons disagree on `Virtual_Server` | Split behavior | **P1** |
| Invalid spec value half-applies | Cluster left broken | **P1** |
| Redeploy or reboot drops the flag | Setting not durable | **P2** |

### 2.5 Reuse from existing CephCI

Suite and helpers already implement the feature doc:

| Doc scenario | Module | Polarion |
|---|---|---|
| 1 — one export, one client, every route | `tests/nfs/regional_mount/nfs_virtual_server_single_export_single_client.py` | CEPH-83632430 |
| 2 — one export, each client on its own route | `tests/nfs/regional_mount/nfs_virtual_server_single_export_multi_client.py` | CEPH-83632431 |
| 3 — several exports, several clients | `tests/nfs/regional_mount/nfs_virtual_server_multi_export_multi_client.py` | CEPH-83632432 |
| 4 — fence one export, one client | `tests/nfs/regional_mount/nfs_virtual_server_export_fencing_single_client.py` | CEPH-83632433 |
| 5 — fence one export; the other stays multi-route | `tests/nfs/regional_mount/nfs_virtual_server_export_fencing_multi_client.py` | CEPH-83632434 |
| RFE — disable, then re-enable | `tests/nfs/regional_mount/nfs_virtual_server_toggle.py` | not assigned |
| VS12 — second toggle cycle | `tests/nfs/regional_mount/nfs_virtual_server_lifecycle.py` | CEPH-83633234 |
| VS13 — multi-daemon conf agrees | same module (`tc: VS13`) | CEPH-83633235 |
| VS14 — invalid spec recovers | same module (`tc: VS14`) | CEPH-83633236 |
| VS15 — redeploy keeps disable | same module (`tc: VS15`) | CEPH-83633237 |

Shared helper: `tests/nfs/regional_mount/virtual_server.py` (`setup_virtual_server_exports`, route discovery, fencing inject, cleanup).

Known gaps in that automation, not in this plan’s pass criteria:

- Scenario 2 failed on `ceph-harish-tfa-0xsa4i` because route discovery required a `/24` pair. node5’s second NIC was never pinged. Treat a repeat of that string as an automation bug.
- Scenarios 4 and 5 are marked `fails due to IBMCEPH-13896`.

### 2.6 Out of scope

- Ingress VIP / HAProxy virtual IP (different feature; `--ingress --virtual-ip`)
- NFS over RDMA
- Kerberos on virtual server
- Proving Ganesha server-owner / clientid identity per destination IP (conf plus successful mount is the QE oracle)
- Expecting extra NICs to stop answering after disable

---

## 3. Environment Prerequisites

### 3.1 Cluster

- cephadm cluster on a build that supports `--enable-virtual-server`
- CephFS volume (suite uses `cephfs`)
- 1 NFS-labeled node with **two or more** IPv4 addresses
- 2+ clients on those same networks for multi-client cases; 1 client is enough for the RFE toggle
- `nfs-utils` on clients

OpenStack example from the suite header:

```bash
python run.py --suite suites/tentacle/nfs/tier-1-nfs-virtual-server-multi-share-export.yaml \
  --global-conf conf/tentacle/nfs/virtual-server-multi-subnet.yaml \
  --rhbuild <build> \
  --custom-config openstack_networks=provider_net_cci_16,provider_net_cci_15
```

### 3.2 Pre-flight

```bash
ceph -v
ceph nfs cluster create --help | grep enable-virtual-server

# On the NFS node and each client — expect 2+ global IPv4 addresses
ip -o -4 addr show
```

If `--enable-virtual-server` is absent, stop. That build cannot run this plan.

If a node has only one usable IPv4 address, stop the multi-route cases. The RFE conf checks still need a virtual-server-capable build, but route cases need the second network.

---

## 4. Test Topology

```text
                +---------------- NFS node ----------------+
                |  NIC A  10.0.1.1     NIC B  10.0.2.1     |
                |  one nfs.<cluster_id> / one Ganesha      |
                +-------------------+----------------------+
                                    |
            export /export_0  (Virtual_Server = true)
                 /                          \
        Client 1 NIC A                   Client 2 NIC B
        10.0.1.2                         10.0.2.2
        mount 10.0.1.1:/export_0         mount 10.0.2.1:/export_0
```

Fencing example (one export only):

```text
EXPORT {
    ...
    Server_Addrs = "10.0.1.1";
    CLIENT {
        Clients = "10.0.1.2";
        Access_Type = RW;
    }
}
```

Only `10.0.1.2` mounting `10.0.1.1` succeeds. Other routes to that export fail. A second export without those blocks still mounts on every route.

---

## 5. Test Cases

### TC-VS0 — Environment / version gate (P0)

**Goal:** Do not run functional cases on a build or lab that cannot host the feature.

**Steps:**

1. Record `ceph -v` and the NFS container image.
2. Confirm `ceph nfs cluster create --help` contains `--enable-virtual-server`.
3. On the NFS node and clients, record `ip -o -4 addr show`. Require 2+ addresses before route cases.

**Pass:**

- [ ] Versions recorded
- [ ] CLI flag present, or the suite skips with a clear message
- [ ] Dual-homed nodes confirmed before TC-VS3 and later

---

### TC-VS1 — Spec acceptance on create (P0)

**Goal:** `--enable-virtual-server` is stored on the service.

**Steps:**

```bash
ceph nfs cluster create cephfs-nfs <nfs-host> --enable-virtual-server
ceph orch ls --service-name nfs.cephfs-nfs --export
```

**Pass:**

- [ ] Create succeeds
- [ ] Exported spec has `enable_virtual_server: true`
- [ ] `ceph nfs cluster ls` shows the new cluster

---

### TC-VS2 — `Virtual_Server` rendered in container conf (P0 — Blocker)

**Goal:** The spec value is what Ganesha actually runs.

**Steps:**

1. After TC-VS1, wait until `ceph orch ps --service-name nfs.cephfs-nfs` shows the daemon running.
2. Read the live conf:

```bash
podman exec "$CID" grep -n Virtual_Server /etc/ganesha/ganesha.conf
```

**Pass:**

- [ ] `Virtual_Server = true;` is present
- [ ] A cluster created **without** `--enable-virtual-server` does not render `Virtual_Server = true`

---

### TC-VS3 — Single export, one client, every route (P0)

**Goal:** Doc scenario 1. One export, one multi-homed client, mount and IO on each server/client route.

**Automation:** `nfs_virtual_server_single_export_single_client.py` (CEPH-83632430).

**Steps:**

1. NFS node and one client, same set of networks, cluster created with `--enable-virtual-server`.
2. Create one export.
3. For each reachable route, mount `vers=4.2` and run write/read IO.
4. Unmount.

**Pass:**

- [ ] Every discovered route mounts
- [ ] IO succeeds on every route
- [ ] No route is skipped because of a subnet guess

---

### TC-VS4 — Single export, multiple clients (P0)

**Goal:** Doc scenario 2. One export. Each client mounts through a different route.

**Automation:** `nfs_virtual_server_single_export_multi_client.py` (CEPH-83632431).

**Steps:**

1. NFS node plus two or more clients, all on the same networks.
2. One export.
3. Mount each client via its own route and run IO.

**Pass:**

- [ ] Each client mounts through a distinct server address
- [ ] IO succeeds on each client
- [ ] A client NIC that can ping the NFS NIC is not dropped by the route picker

---

### TC-VS5 — Multiple exports, multiple clients (P0)

**Goal:** Doc scenario 3. Several exports. Each client uses a distinct route.

**Automation:** `nfs_virtual_server_multi_export_multi_client.py` (CEPH-83632432).

**Steps:**

1. Same multi-network lab as TC-VS4.
2. Create one export per client (suite uses `export_num: 2`).
3. Mount each export on its client route and run IO.

**Pass:**

- [ ] Each export mounts on its route
- [ ] IO succeeds on each mount

---

### TC-VS6 — Export fencing, one client (P0)

**Goal:** Doc scenario 4. `Server_Addrs` and `CLIENT` allow only one server IP and one client IP.

**Automation:** `nfs_virtual_server_export_fencing_single_client.py` (CEPH-83632433). **Currently fails: [IBMCEPH-13896](https://ibm-ceph.atlassian.net/browse/IBMCEPH-13896).**

**Steps:**

1. One export on a virtual-server cluster.
2. Apply an export conf equivalent to:

```text
Server_Addrs = "<server-ip-A>";
CLIENT {
    Clients = "<client-ip-A>";
    Access_Type = RW;
}
```

3. Mount from client IP A to server IP A. Expect success and IO.
4. Mount the same export via every other route. Expect failure.

**Pass:**

- [ ] Allowed pair mounts and IO succeeds
- [ ] Every other route to that export fails
- [ ] Failure is an NFS mount error, not a test-harness timeout

---

### TC-VS7 — Export fencing, multiple clients (P0)

**Goal:** Doc scenario 5. Fence one export. A second export on the same cluster stays multi-route, including from a different client.

**Automation:** `nfs_virtual_server_export_fencing_multi_client.py` (CEPH-83632434). **Currently fails: [IBMCEPH-13896](https://ibm-ceph.atlassian.net/browse/IBMCEPH-13896).**

**Steps:**

1. Two exports, two clients, virtual server enabled.
2. Fence export 1 to one server IP and one client IP.
3. Export 1: allowed pair succeeds; other routes and the other client fail.
4. Export 2: both clients mount on their routes and run IO.

**Pass:**

- [ ] Fenced export accepts only the configured pair
- [ ] Unfenced export still mounts from each client route

---

### TC-VS8 — Disable virtual server on the existing cluster (P0 — Blocker, IBMCEPH-11530)

**Goal:** Turn the feature off without deleting the cluster or the export.

**Automation:** `nfs_virtual_server_toggle.py` (first half). No Polarion ID yet (Scenario 6).

**Steps:**

1. Start from TC-VS1 / TC-VS2 (`enable_virtual_server: true`, `Virtual_Server = true`).
2. Note `ceph nfs cluster ls` and `ceph nfs export ls <cluster_id>`.
3. Export the service, set `enable_virtual_server: false`, apply, reconfig:

```bash
ceph orch ls --service-name nfs.<cluster_id> --export > nfs.yaml
# set spec.enable_virtual_server: false
ceph orch apply -i nfs.yaml
ceph orch daemon reconfig <nfs-daemon>
```

4. Re-export the spec and re-read `ganesha.conf`.
5. Confirm the same cluster id and the same export name are still listed.

**Pass:**

- [ ] `ceph orch apply` and daemon reconfig succeed
- [ ] Exported spec does not have `enable_virtual_server: true` (key absent or `false`)
- [ ] `Virtual_Server = false;` in the container conf
- [ ] Cluster id unchanged
- [ ] Export still listed

---

### TC-VS9 — NFS data path survives disable (P0 — Blocker, IBMCEPH-11530)

**Goal:** The share still serves after the flag flips.

**Steps:**

1. Complete TC-VS8.
2. Mount the existing export from a client to the NFS node address (NFSv4.2).
3. Write, read, and unmount.
4. `ceph orch ps` still shows the NFS daemon running. No crash loop.

**Pass:**

- [ ] Mount and IO succeed while `Virtual_Server = false`
- [ ] Daemon stays up
- [ ] Extra NICs are still allowed to answer; do not fail this case because a second route mounted

---

### TC-VS10 — Re-enable, including a second route (P0 — Blocker, IBMCEPH-11530)

**Goal:** The toggle is reversible and the feature works again.

**Steps:**

1. From the disabled cluster in TC-VS9, apply `enable_virtual_server: true` and `ceph orch daemon reconfig`.
2. Confirm spec `enable_virtual_server: true` and `Virtual_Server = true;`.
3. Mount and run IO on at least two different server addresses (two routes).
4. Confirm the export name did not change.

**Pass:**

- [ ] Conf and spec are true again
- [ ] IO succeeds on two routes after re-enable
- [ ] Same cluster and export as before the first disable

---

### TC-VS11 — Default cluster is not virtual-server (P0)

**Goal:** The flag is opt-in. A normal `ceph nfs cluster create` does not turn it on.

**Dev review (9.2):** The `Virtual_Server` line is **always** present in the NFSv4 block, even for default clusters. Grep for the **value** (`Virtual_Server = false;`), not for absence of the line.

**Steps:**

1. Create a second NFS cluster with no `--enable-virtual-server`.
2. Export its spec and read `ganesha.conf`.

**Pass:**

- [ ] Spec does not set `enable_virtual_server: true`
- [ ] Conf contains `Virtual_Server = false;` (line is present; value is false)

---

### TC-VS12 — Toggle true → false → true leaves one daemon generation (P1)

**Goal:** Repeated orch updates do not orphan containers or drop the export.

**Automation:** `nfs_virtual_server_lifecycle.py` (`tc: VS12`). Polarion: CEPH-83633234.

**Steps:**

1. Repeat TC-VS8 and TC-VS10 once more on the same cluster.
2. After each transition, check `ceph orch ps` for a single running NFS daemon (or the placement count you deployed) and `ceph nfs export ls`.

**Pass:**

- [ ] Each transition matches TC-VS8 or TC-VS10
- [ ] No extra `nfs.<id>` daemons left behind
- [ ] Export list is stable across both toggles

---

### TC-VS13 — Multi-daemon conf agrees (P1)

**Goal:** Every NFS daemon for the service renders the same `Virtual_Server` value.

**Automation:** `nfs_virtual_server_lifecycle.py` (`tc: VS13`). Polarion: CEPH-83633235. Skips (pass) if fewer than 2 running NFS daemons.

**Steps:**

1. Placement count 2, if the lab allows two NFS nodes that are both multi-homed.
2. Enable, then disable.
3. On each daemon host, grep `Virtual_Server` in that container’s conf.

**Pass:**

- [ ] Both daemons show `true` after enable
- [ ] Both show `false` after disable
- [ ] Skip this case when the lab has a single NFS node, and say so in the evidence

---

### TC-VS14 — Invalid spec value (P1)

**Goal:** A bad bool does not leave a lasting broken NFS data path.

**Automation:** `nfs_virtual_server_lifecycle.py` (`tc: VS14`). Polarion: CEPH-83633236.

**Dev review (9.2):** There is **no** runtime validation on `enable_virtual_server`. A bad value like `"maybe"` is accepted by `ceph orch apply` and only fails later at the Ganesha layer. Do not expect orch to reject the apply.

**Steps:**

1. From a good `enable_virtual_server: true` spec, apply `enable_virtual_server: "maybe"`.
2. Capture that orch apply succeeds (no schema rejection).
3. Run `ceph orch daemon reconfig` and observe Ganesha-side failure / conf behavior.
4. Restore a good `enable_virtual_server: true|false` and reconfig; confirm the export is still mountable.

**Pass:**

- [ ] Orch apply of the bad value is accepted (expected)
- [ ] Failure surfaces at Ganesha / reconfig, not as an orch schema error
- [ ] After restoring a valid bool + reconfig, the last good export is still mountable

---

### TC-VS15 — Redeploy keeps the disabled setting (P2)

**Goal:** The operator’s last choice survives `ceph orch redeploy`.

**Automation:** `nfs_virtual_server_lifecycle.py` (`tc: VS15`). Polarion: CEPH-83633237.

**Steps:**

1. Leave the cluster at `enable_virtual_server: false` and confirm `Virtual_Server = false`.
2. `ceph orch redeploy nfs.<cluster_id>`.
3. Wait for the daemon and re-check spec and conf.

**Pass:**

- [ ] Spec is still not `enable_virtual_server: true`
- [ ] Conf is still `Virtual_Server = false`
- [ ] Export mounts

---

## 6. Execution Order

| Phase | Cases | Gate |
|---|---|---|
| Gate | TC-VS0 | Stop if the CLI flag or the second network is missing |
| Spec + render | TC-VS1, TC-VS2, TC-VS11 | **Stop if conf does not render** |
| Feature routes | TC-VS3, TC-VS4, TC-VS5 | **Stop if a route cannot mount** |
| Fencing | TC-VS6, TC-VS7 | Required for feature sign-off; track IBMCEPH-13896 separately |
| RFE | TC-VS8, TC-VS9, TC-VS10 | **Stop if disable deletes the cluster or conf does not change** |
| Lifecycle | TC-VS12, TC-VS13, TC-VS14 | High |
| Durability | TC-VS15 | Best-effort |

**Feature sign-off (ISCE-1982):** TC-VS2 + TC-VS3 + TC-VS4 + TC-VS5. TC-VS6 and TC-VS7 when IBMCEPH-13896 is fixed.

**RFE sign-off (IBMCEPH-11530):** TC-VS8 + TC-VS9 + TC-VS10.

---

## 7. Pass / Fail Matrix

| ID | Assertion | Priority | Automation |
|---|---|---|---|
| VS0 | Version and dual-home gate | P0 | `validate_virtual_server_support` |
| VS1 | `--enable-virtual-server` stored on the spec | P0 | toggle setup |
| VS2 | `Virtual_Server = true` in container conf | **P0 Blocker** | toggle setup |
| VS3 | One export, one client, all routes | P0 | CEPH-83632430 |
| VS4 | One export, multiple clients | P0 | CEPH-83632431 |
| VS5 | Multiple exports, multiple clients | P0 | CEPH-83632432 |
| VS6 | Fence one export, one client | **P0** | CEPH-83632433, IBMCEPH-13896 |
| VS7 | Fence one export; other export stays open | **P0** | CEPH-83632434, IBMCEPH-13896 |
| VS8 | Disable keeps cluster and sets conf false | **P0 Blocker** | `nfs_virtual_server_toggle.py` |
| VS9 | IO still works after disable | **P0 Blocker** | same module |
| VS10 | Re-enable restores true and a second route | **P0 Blocker** | same module (second route is the gap to add) |
| VS11 | Default create renders `Virtual_Server = false` | P0 | `nfs_virtual_server_toggle.py` |
| VS12 | Second toggle does not orphan daemons | P1 | CEPH-83633234 / `nfs_virtual_server_lifecycle.py` |
| VS13 | All daemons share the flag | P1 | CEPH-83633235 / same |
| VS14 | Bad value accepted by orch; fails at Ganesha | P1 | CEPH-83633236 / same |
| VS15 | Redeploy preserves disable | P2 | CEPH-83633237 / same |

---

## 8. Suggested CephCI Automation Shape

| Item | Recommendation |
|---|---|
| Suite | Keep `suites/tentacle/nfs/tier-1-nfs-virtual-server-multi-share-export.yaml` |
| Conf | `conf/tentacle/nfs/virtual-server-multi-subnet.yaml` plus two provider networks |
| RFE module | `tests/nfs/regional_mount/nfs_virtual_server_toggle.py` — one module is enough for VS8–VS10 because they share one cluster |
| Helpers | Stay in `tests/nfs/regional_mount/virtual_server.py` |
| Polarion | Existing IDs for VS3–VS7. Add one ID for the RFE module and link IBMCEPH-11530 |
| CI minimum | VS0, VS2, VS3, VS8, VS9, VS10 |
| Do not block RFE on | VS6 / VS7 until IBMCEPH-13896 is fixed |

The route-discovery `/24` miss on scenario 2 is an automation fix, separate from this RFE.

---

## 9. Evidence to Attach on the Jira

For an IBMCEPH-11530 run:

1. Ceph version and NFS image id
2. `ip -o -4 addr show` from the NFS node and the client
3. Exported spec before disable, after disable, and after re-enable
4. `Virtual_Server` line from container `ganesha.conf` at those three points
5. `ceph nfs cluster ls` and `ceph nfs export ls` showing the same names throughout
6. Mount and IO success after disable, and on two routes after re-enable
7. Go/No-Go against TC-VS8, TC-VS9, and TC-VS10

For an ISCE-1982 run, add per-route mount logs for TC-VS3–VS5 and the fencing results for TC-VS6–VS7, including the IBMCEPH-13896 status.

---

## 10. References

- Feature: [ISCE-1982](https://jsw.ibm.com/browse/ISCE-1982)
- RFE: [IBMCEPH-11530](https://ibm-ceph.atlassian.net/browse/IBMCEPH-11530)
- BZ: [2370079](https://bugzilla.redhat.com/show_bug.cgi?id=2370079)
- Fencing bug: [IBMCEPH-13896](https://ibm-ceph.atlassian.net/browse/IBMCEPH-13896)
- Ceph NFS service: https://docs.ceph.com/en/latest/cephadm/services/nfs/
- Suite: `suites/tentacle/nfs/tier-1-nfs-virtual-server-multi-share-export.yaml`
- Helper: `tests/nfs/regional_mount/virtual_server.py`

---

## 11. Developer review notes (9.2 downstream)

Reviewed against 9.2 downstream code — plan confirmed accurate; proceed.

| Topic | Confirmed |
|---|---|
| Toggle mechanism | No separate CLI. Export spec → edit `enable_virtual_server` → `ceph orch apply -i` → **`ceph orch daemon reconfig` (mandatory)**. Field is not in the dependency tracker, so auto-reconfig will not fire. |
| `ganesha.conf` | `Virtual_Server = true/false;` renders unconditionally in the `NFSv4 {}` block. Default clusters show `Virtual_Server = false;`. |
| Cluster / export preservation | Only `ganesha.conf` changes. Exports (RADOS) and cluster are untouched. Extra NICs keep answering after disable — that is fencing territory, not disable. |

Minor corrections applied above: TC-VS11 (grep value, not line absence); TC-VS14 (orch accepts bad values; fail at Ganesha).

---

*Document version: 1.1 — QE plan for Regional File Mount (scenarios 1–5) and the IBMCEPH-11530 disable/re-enable RFE. Updated after 9.2 developer review. Disable does not mean extra NICs stop serving; fencing covers that.*
