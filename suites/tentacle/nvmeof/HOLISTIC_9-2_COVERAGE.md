# IBM Ceph 9.2 NVMeoF Holistic Coverage Map

**Holistic suites:**  
- Smoke: `tier-2_nvmeof_9-2_holistic_smoke.yaml` (`fio_runtime: 300`)  
- Full: `tier-2_nvmeof_9-2_holistic_cross-feature.yaml` (`fio_runtime: 600`)  
- Soak: `tier-2_nvmeof_9-2_holistic_soak.yaml` (`fio_runtime: 1800` + backup restore burst)  

**Conf:** `conf/tentacle/nvmeof/ceph_nvmeof_9-2_holistic.yaml`  
**Orchestrator:** `tests/nvmeof/test_ceph_nvmeof_holistic_io.py`  
**Workload filter:** `HOLISTIC_9-2_WORKLOADS.md` (customer apps → FIO personas)  

Under-load: OLTP / VM+crc32c / AI-scan / backup-media personas; `assert_listeners` after LB `scale_up`.

This is a **customer-like system / soak** suite. It does **not** replace every tentacle NVMeoF suite. Use this matrix to decide what still needs companion runs.

Legend:

| Tag | Meaning |
|-----|---------|
| **H-full** | Exercised end-to-end in holistic (configure + validate/IO) |
| **H-partial** | Present but thinner than the dedicated suite |
| **Companion** | Keep running the listed suite(s); not suitable for standing “all-features” env |

---

## ISCE / RFE → holistic status

| Item | Status | Holistic coverage | Companion suite(s) |
|------|--------|-------------------|--------------------|
| ISCE-2203 Auto-add listeners | H-partial | Phase 2 listeners + day2 `assert_listeners` after LB scale_up | `tier-2_nvmeof_e2e_refresh-network.yaml` |
| ISCE-3542 DHCHAP key per host | H-full | Phase 4d same-phase key create + auth FIO; cnode5 also configured | `tier-2_nvmeof_9-1_feature.yaml`, inbandauth suites |
| ISCE-2205 GW locale identification | Companion | — | *(no dedicated tentacle suite found)* |
| ISCE-1424 Performance monitor | Companion | — | *(no dedicated suite; QoS/IO stats partial in day2)* |
| ISCE-2122 Failover time | H-partial | day2 `ha_failover` under IO | `tier-1_nvmeof_ha_sanity.yaml`, `tier-2_nvmeof_4nodes_gateway_ha_tests.yaml` |
| ISCE-2771 CSI driver | Companion | — (not DS) | out of cephci NVMeoF suites |
| ISCE-2772 Metadata pool automation | H-full | `nvmeof_metadata` pos_args (not rbd) | sanity / gateway deploy suites |
| ISCE-2682 Initiator interop | Companion | — | platform / interop outside this suite |
| ISCE-3027 SPDK CRC reuse | Companion | — | *(no dedicated suite)* |
| 3127 RADOS namespace in GWs | H-full | cnode7 + tenant FIO | `tier-2_nvmeof_9-1_feature.yaml` |
| ISCE-3333 SPDK 25.09 | Companion | — | upgrade / build pipeline |
| ISCE-2117 NVMEoF Cancel | Companion | — | *(no dedicated suite)* |
| ISCE-2121 Read-only NS | H-full | Phase 4c `test_ceph_nvmeof_readonly_ns.py` | `tier-2_nvmeof_functional_Regression.yaml` |
| ISCE-2119 4K namespaces | Companion | — | *(no dedicated suite found)* |
| ISCE-2086 NS reservations | H-full | Phase 4 lifecycle + Phase 4b multi-init types | `tier-2_nvmeof_e2e_ns-reservation.yaml` |
| ISCE-822 / Ceph CLI | H-partial | via gateway / masking / qos / resize CLIs | `tier-2_nvmeof_gateway_operations.yaml` |
| ISCE-1885 Resize workflow | H-partial | day2 `resize` under IO | `tier-2_nvmeof_functional_Regression.yaml` (ns_resize) |
| 1 GW per node restriction | H-partial | topology assumes 1 GW role/node | cephadm / neg placement tests |
| Stretch Cluster HA | Companion | — | stretch / multi-site suites |
| Scale and Longevity | Companion | smoke suite `fio_runtime: 300`; full `600` / soak `1800` | tier-3 scale suites |
| Auto Namespace Load Balancing | H-full | day2 scale_down/up | `tier-2_nvmeof_1gwgroup_8gwnodes_loadbalancing_tests.yaml` |
| In-band authentication | H-full | cnode5 + Phase 4d DHCHAP FIO | inbandauth suites |
| Per-namespace masking | H-full | Phase 3 setup + day2 visibility churn | `tier-2_nvmeof_e2e_ns-masking.yaml`, `tier-2_8-1-nvmeof_cross-feature.yaml` |
| Upgrade Paths | Companion | — | `tier-2_nvmeof_*upgrade*.yaml` |
| VMware vSphere Plugin | Companion | — | VMware client suites |
| Dashboard multi-NS workflow | Companion | — | dashboard suites |
| Events and Alerting | Companion | intentional break | `tier-2_nvmeof-alerts_health-checks-events.yaml` |
| Multi-tenancy | H-partial | tenant ACL/masking/auth partition | masking + inbandauth + gwgroup suites |
| FC → NVMe/TCP migration | Companion | — | *(no dedicated suite)* |
| Backup ISV / SOS / call-home | Companion | — | *(no dedicated suite)* |
| QoS | H-full | day2 `qos_validate_io` (set + iostat validate + relax) | `tier-2_nvmeof_qos_sanity_tests.yaml` |
| Colocated GW + OSD | H-full | node4/node5 in conf | functional regression colocated TCs |
| Multi-client realistic FIO | H-full | Phase 5 under-load (+ crc32c integrity client) | sanity / functional |
| Initiator reboot autoconnect | H-full | Phase 4.5 `--persistent` + discovery.conf + nvmf-autoconnect; no manual reconnect | `tier-3_nvmeof_restart_operations.yaml` (manual reconnect path) |

---

## Tentacle suite folder → holistic disposition

| Suite | Disposition |
|-------|-------------|
| `tier-1_nvmeof_sanity.yaml` | Companion (basic E2E depth) |
| `tier-1_nvmeof_ha_sanity.yaml` | Companion (HA depth) |
| `tier-1_nvmeof_plugin_test_bed.yaml` | Pattern borrowed (no cleanup) |
| `tier-1_nvmeof_4-nvmeof-gwgroup_2gw_tests.yaml` | Companion (multi-group) |
| `tier-2_8-1-nvmeof_cross-feature.yaml` | Pattern borrowed; masking covered in holistic |
| `tier-2_nvmeof_9-2_holistic_cross-feature.yaml` | **Full holistic** |
| `tier-2_nvmeof_9-2_holistic_smoke.yaml` | **Smoke holistic** (same conf) |
| `tier-2_nvmeof_9-2_holistic_soak.yaml` | **Soak holistic** (1800s personas + restore burst) |
| `tier-2_nvmeof_9-1_feature.yaml` | Companion depth; RADOS/DHCHAP also in holistic |
| `tier-2_nvmeof_e2e_ns-masking.yaml` | Companion depth; H-full core in holistic |
| `tier-2_nvmeof_e2e_ns-reservation.yaml` | Companion depth; H-full core + multi-init in holistic |
| `tier-2_nvmeof_qos_sanity_tests.yaml` | Companion depth; H-full core via `qos_validate_io` |
| `tier-2_nvmeof_1gwgroup_8gwnodes_loadbalancing_tests.yaml` | Companion depth; H-full core scale story |
| `tier-2_nvmeof_4nodes_gateway_ha_tests.yaml` | Companion (HA matrix / mTLS / tools) |
| `tier-2_nvmeof_4-nvmeof-gwgroup_inbandauth*.yaml` | Companion (multi-group auth + HA) |
| `tier-2_nvmeof_functional_Regression.yaml` | Companion (readonly/resize/data integrity depth) |
| `tier-2_nvmeof_gateway_operations.yaml` | Companion (CLI OMAP matrix) |
| `tier-2_nvmeof_e2e_refresh-network.yaml` | Companion (baremetal dual-NIC) |
| `tier-2_nvmeof-alerts_health-checks-events.yaml` | Companion (destructive alerts) |
| `tier-2_nvmeof_hugepages_operations.yaml` | Companion |
| `tier-2_nvmeof_rest_sanity.yaml` | Companion |
| `tier-2_nvmeof_rbd_mirror.yaml` | Companion (dual cluster) |
| `tier-2_nvmeof_e2e_fips.yaml` | Companion |
| `tier-2_nvmeof_*upgrade*.yaml` / b2b | Companion |
| `tier-2_nvmeof_16K_namespace_masking_*.yaml` | Companion (scale extreme) |
| All `tier-3_*` scale / restart / operational | Companion |
| All `tier-4_*` neg / ns ops | Companion |

---

## Holistic phase checklist

| Phase | What |
|-------|------|
| 0 | prereq + full Ceph (mon/mgr/osd/mds/rgw) + 4 clients |
| 1 | `rbd` data + `nvmeof_metadata` + RADOS NS + NVMeoF on node4–7 |
| 2 | 8 tenant subsystems (ACL mix, inband shell, RADOS, 100G NS) |
| 3 | NS masking setup |
| 4 | Reservation lifecycle |
| 4b | Multi-initiator reservation types |
| 4c | Read-only namespaces |
| 4d | DHCHAP per-host authenticated FIO (same-phase keys) |
| 4e | Real DB engines on NVMe mounts (PG/MySQL/MariaDB/Mongo/Redis[/Cassandra]) |
| 4.5 | Initiator reboot + `nvmf-autoconnect` (UUIDs return without manual discover/connect) |
| 5 | Customer FIO personas (OLTP/VM/AI/backup) + day2 (QoS, visibility, resize, LB + listener assert, HA; soak: fio_burst) |

---

## How to use

- **CI / PR gate:** smoke suite.
- **Release system qualification:** full suite; **soak** for longevity + restore burst.
- **Customer workload mapping:** `HOLISTIC_9-2_WORKLOADS.md`.
- **Feature depth / regressions:** companion suites from the tables above.
- **Do not expect** holistic alone to gate upgrades, FIPS, mirror, alerts, 16K scale, VMware, CSI, or Dashboard.
