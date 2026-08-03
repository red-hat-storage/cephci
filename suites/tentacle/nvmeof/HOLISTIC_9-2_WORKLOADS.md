# IBM Ceph 9.2 Holistic — Customer Workload Applicability

cephci NVMeoF automation drives **Linux initiators + FIO against NVMe namespaces**.
It does **not** run real VMware guests, CSI pods, SAP/Oracle, or backup ISVs.
Where a customer workload fits, we **emulate** it with FIO personas under day-2 load.

**Suites (same conf `ceph_nvmeof_9-2_holistic.yaml`):**

| Profile | Suite | IO window |
|---------|-------|-----------|
| Smoke | `tier-2_nvmeof_9-2_holistic_smoke.yaml` | ~300s |
| Full | `tier-2_nvmeof_9-2_holistic_cross-feature.yaml` | ~600s |
| Soak | `tier-2_nvmeof_9-2_holistic_soak.yaml` | ~1800s + restore burst |

---

## Customer workload filter

| Workload | Fit in cephci? | Holistic treatment | Notes |
|----------|----------------|--------------------|-------|
| **Virtual machines** (KVM-like block) | **Emulate** | node10: 4k `randrw` 85/15 + `crc32c` | Real VMware/ESX/OpenStack/Proxmox guests → companion / manual |
| **Databases** (OLTP) | **Real + emulate** | Phase 4e real engines on NVMe mounts; node9 also FIO OLTP persona | See engines table below |
| **Kubernetes CSI PVs** | **Out** | Companion | Needs K8s + CSI driver; not in this framework |
| **AI/ML infrastructure** | **Emulate** | node11: 1M `randread`, qd32 | Dataset/checkpoint *scan* only; no GPU training stack |
| **HPC** | **Partial** | Multi-client parallel personas | No MPI/Lustre/parallel FS; concurrent block IO only |
| **Enterprise apps** (SAP/Oracle/MSSQL/ERP) | **Emulate as DB** | Same as OLTP persona | App stacks out of scope |
| **Backup repositories** | **Emulate** | node12: 1M seq `write`; soak `fio_burst` seq `read` | No Commvault/Veeam; restore modeled as large read burst |
| **VDI** | **Partial** | VM-boot persona approximates one desktop disk | No broker / boot-storm scale (only 4 clients) |
| **Media production** | **Emulate** | node12 large seq write (+ soak restore read) | No NLE/transcode apps |
| **SDS / HCI appliances** | **Topology** | Conf has GW colocated with OSD (node4/5) | No third-party HCI product under test |

---

## Phase 5 FIO personas (in suite)

| Client | Persona | FIO shape | Why |
|--------|---------|-----------|-----|
| node9 | OLTP database | `randrw` 70/30, bs 4k, iodepth 32, num_jobs 4, fsync 32, direct | Low-latency mixed R/W |
| node10 | VM boot + integrity | `randrw` 85/15, bs 4k, verify crc32c | Boot/OS disk + corruption detect under day-2 |
| node11 | AI / analytics | `randread`, bs 1M, iodepth 32, num_jobs 2 | Large-block dataset scan |
| node12 | Backup / media ingest | `write`, bs 1M, iodepth 8 | Sequential ingest / scratch |

Day-2 under these personas: QoS validate, NS visibility, resize, LB + listener assert, HA failover.  
**Soak only:** `fio_burst` sequential read on node12 (backup restore under load).

**Phase 4.5 (before personas):** reboot node10 after persistent connect; assert namespace UUIDs return via `nvmf-autoconnect` / `discovery.conf` without manual discover/connect (short FIO smoke).

---

## Phase 4e — Real database engines (podman on NVMe FS)

Module: `tests/nvmeof/test_ceph_nvmeof_db_workloads.py`  
Workflow: `tests/nvmeof/workflows/db_workloads.py`

Each engine gets its own NVMe namespace → `mkfs.xfs` → mount → podman with datadir on the mount → native bench.

Namespace budget (`cnode_db` `bdevs.count`, `max_ns: 64`):

| Profile | Namespaces | Size | Engines using NS | Spare |
|---------|------------|------|------------------|-------|
| Smoke | 8 | 500G | 5 | 3 |
| Full | 10 | 500G | 6 | 4 |
| Soak | 10 | 500G | 6 | 4 |

Skipped engines (Oracle) do **not** consume a namespace.

Standing tenant namespaces (Phase 2) are also **500G**: cnode3/4/5/8 × **8 NS**, cnode6 × **4**, cnode7 RADOS × **4+4**, masking × **16** per subsystem. Conf OSD nodes use **12 × 1000G** disks to back this.

| Engine | In suite? | How IO is driven | Notes |
|--------|-----------|------------------|-------|
| **PostgreSQL** | Yes | `pgbench` init + timed run | docker.io/library/postgres:16 |
| **MySQL** | Yes | load + `mysqlslap` | docker.io/library/mysql:8.4 |
| **MariaDB** | Yes | load + `mysqlslap` | docker.io/library/mariadb:11.4 |
| **MongoDB** | Yes | insertMany + timed update/find | docker.io/library/mongo:7 |
| **Redis persistence** | Yes | AOF on mount + `redis-benchmark` | docker.io/library/redis:7 |
| **Cassandra** | Full + soak | cql inserts/selects | Omitted from **smoke** (slow start) |
| **Oracle Database** | **Skipped** | — | OTN/license; listed in suite only to document skip |
| **Microsoft SQL Server** | **Not enabled** | Optional `accept_eula: true` path exists | Enable manually if lab policy allows |

Requires client network pull of container images + `podman` (installed on demand).

---

## Extra workloads worth adding later (cephci-feasible)

| Idea | Value | Effort |
|------|-------|--------|
| **DB journal companion** | Second path: 4k `randwrite` high sync | Low — 5th NS or second FIO on same client |
| **Boot storm** | Many short 4k `randread` jobs | Med — more initiators or high `num_jobs` |
| **Trim / discard under load** | Thin-provision / SSD reclaim | Low if `discard` io_type wired |
| **FS-mounted app IO** | ext4/xfs + file FIO (closer to VMs) | Med — pattern exists in `test_ceph_nvmeof_data_integrity.py` |
| **Snapshot/clone under IO** | VDI/backup story via RBD snap | Med — new day2 op |
| **Dual-pattern QoS contention** | OLTP vs backup fighting QoS | Low — already partial via personas + qos_validate |
| **Cold restore then verify** | `fio_burst` write → read → crc32c | Low — compose day2 ops |
| **Multi-path ANA churn** | Failover while all personas run | Partial today via `ha_failover` |

---

## Explicitly not in holistic (companion / external)

- Kubernetes CSI + stateful apps  
- VMware ESX guest IO (ESX connect-only suite exists elsewhere)  
- OpenStack Cinder attach path  
- SAP / Oracle / Microsoft SQL application suites  
- VDI broker pools  
- Backup vendor integrations  
- Stretch / multi-site / mirror dual-cluster depth  

See also: `HOLISTIC_9-2_COVERAGE.md`
