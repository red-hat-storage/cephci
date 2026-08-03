# IBM Cloud BareMetal Bug → Test Coverage (filter 10895)

**Filter:** [10895 — IBM cloud bugs](https://ibm-ceph.atlassian.net/issues/?filter=10895)  
**Auth:** Jira REST `POST /rest/api/3/search/jql` as `rahul.lepakshi@ibm.com` (token not stored in repo).  
**Pulled:** 2026-08-03  
**Hardened:** 2026-08-03 — quality pass (assertions / IO continuity / field fixes)

## How bugs were sourced

Rovo / Jira filter **10895** (“IBM cloud bugs”) was used to enumerate open BareMetal / `cloud-baremetal` NVMeoF bugs on ibm-ceph Atlassian. Filter metadata + issue list were pulled via the Jira REST API (`GET /rest/api/3/filter/10895`, then search with that JQL; credentials are local only and are **not** stored in this repo). Issues were then theme-grouped, gap-analyzed against existing cephci coverage, and mapped into the suite/modules below.

## Filter definition

| Field | Value |
|-------|-------|
| ID | **10895** |
| Name | **IBM cloud bugs** |
| JQL | `status IN (ASSIGNED, "Awaiting Feedback", "Blocked (migrated)", "In Progress", MODIFIED, "Need Information", New, ON_DEV, POST, Review, "To Do", Reviewing, ON_QA) AND labels IN (cloud-baremetal, BareMetal) AND type = Bug ORDER BY priority DESC` |

**Open bugs in filter:** **13**

---

## Full bug list (filter 10895)

| Key | Pri | Status | Summary |
|-----|-----|--------|---------|
| IBMCEPH-17250 | Blocker | ON_QA | CLONE - CLONE - Maintenance mode enter/exit results in gateway not receiving data traffic |
| IBMCEPH-16125 | Blocker | New | CLONE - Maintenance mode enter/exit results in gateway not receiving data traffic |
| IBMCEPH-16124 | Blocker | MODIFIED | CLONE - Maintenance mode enter/exit results in gateway not receiving data traffic |
| IBMCEPH-15819 | Blocker | MODIFIED | Maintenance mode enter/exit results in gateway not receiving data traffic |
| IBMCEPH-16447 | Major | ON_QA | [9.1z Backport] NVMeoF - avoid having plaintext keys in the nvmeof service spec file |
| IBMCEPH-17223 | Normal | MODIFIED | Removing host during PSK rotation with keep-connections causes other GWs to restart/crash |
| IBMCEPH-17197 | Normal | MODIFIED | Removing connected host from subsystem causes OMAP-sync GWs to crash/restart |
| IBMCEPH-17007 | Normal | POST | CLONE - disable/enable NVMe-oF gateways during host maintenance |
| IBMCEPH-16374 | Normal | New | Scale down on a gateway group shows a gateway node in DELETING |
| IBMCEPH-16261 | Normal | MODIFIED | disable/enable NVMe-oF gateways during host maintenance |
| IBMCEPH-15815 | Normal | New | NVMEoF Gateway failover taking longer than expected |
| IBMCEPH-15769 | Normal | In Progress | namespaces were assigned a wrong anagrpid |
| IBMCEPH-15679 | Normal | New | Add support to upgrade nvmeof daemons using staggered upgrade `--daemon-types` |

---

## Themes (ROI order)

1. **Host maintenance × NVMeoF IO path** (Blockers 15819 + clones) — highest ROI  
2. **GW disable/enable around maintenance** (16261 / 17007)  
3. **Host ACL remove / keep-connections stability** (17197 / 17223)  
4. **ANA / anagrpid integrity** (15769)  
5. **Scale-down lifecycle stuck DELETING** (16374)  
6. **Failover SLO** (15815)  
7. **No plaintext keys in nvmeof orch spec** (16447)  
8. **Staggered upgrade `--daemon-types nvmeof`** (15679)

---

## Quality gap analysis (pre-harden → fix)

| Area | What was weak | What we hardened |
|------|---------------|------------------|
| Maintenance (15819) | Soft-swallowed `nvme-gw disable/enable`; string-search for `"failed"`; no `validate_io` during/after | Split pure vs disable/enable TCs; hard `Availability=AVAILABLE`; `validate_io` before/during/after; peer restart detection via container_id/started |
| Disable/enable (16261) | Combined with blocker TC and soft-failed, so 16261 was effectively untested | Dedicated TC with `gw_disable_enable: true` and hard fail on disable/enable errors |
| Host remove (17197/17223) | “Restart” helper only listed orch status; keep-connections treated FIO fail as OK for both paths | Real container_id/started restart detection; keep-connections **requires** FIO success; ACL path asserts host gone |
| ANA (15769) | Looked for `anagrp-id` keys — real field is `load_balancing_group`; skipped missing ANA | Read `load_balancing_group`; fail on missing; optional `extra_ns_churn` re-assert |
| Scale-down (16374) | Blob string search only; no IO; no remaining-AVAILABLE / health clear | Structured `Availability==DELETING`; remaining AVAILABLE; `NVMEOF_GATEWAY_DELETING` health clear; optional IO via initiators |
| Failover SLO (15815) | Timed failover but soft on IO; no `validate_io` | Hard `validate_io` before/after failover+failback; SLO remains warn-by-default (`fail_on_slo: false`) |
| Spec keys (16447) | Static export scan — adequate | Unchanged (probe-quality OK) |
| daemon-types (15679) | Acceptance probe only — adequate | Unchanged; full staggered upgrade stays in companion upgrade suites |

---

## Test-miss analysis → suite TC mapping

| Key(s) | Coverage | Suite TC / module | Notes |
|--------|----------|-------------------|-------|
| 15819, 16124, 16125, 17250 | **Hardened** | `Host maintenance under NVMeoF IO` → `test_ceph_nvmeof_host_maintenance_io.py` | `gw_disable_enable: false`; IO + AVAILABLE + peer stability |
| 16261, 17007 | **Hardened** | `Maintenance with nvme-gw disable/enable` → same module | `gw_disable_enable: true`; disable/enable hard-fail |
| 17197 | **Hardened** | `Host remove under IO stability` → `test_ceph_nvmeof_host_remove_stability.py` | Restart detection; ACL removed |
| 17223 | **Hardened** | `Host remove keep-connections stability` → same module | `keep_connections: true`; FIO must continue |
| 15769 | **Hardened** | `ANA group id integrity` → `test_ceph_nvmeof_anagrp_integrity.py` | `load_balancing_group` ∈ `[1..num_gws]` + churn |
| 16374 | **Hardened** | `Scale-down DELETING lifecycle` → `test_ceph_nvmeof_gw_scale_deleting.py` | DELETING clear + remaining AVAILABLE + IO |
| 15815 | **Hardened** | `Failover SLO check` → `test_ceph_nvmeof_failover_slo.py` | IO hard gate; SLO warn default |
| 16447 | **Covered** | `Spec no plaintext keys` → `test_ceph_nvmeof_spec_no_plaintext_keys.py` | Export orch nvmeof specs; reject PEM private keys |
| 15679 | **Covered (probe)** | `daemon-types nvmeof upgrade probe` → `test_ceph_nvmeof_daemon_types_upgrade.py` | Reject “unexpected daemon type nvmeof”; stop if started |

---

## Suite / modules

| Artifact | Path |
|----------|------|
| Coverage map | `suites/tentacle/nvmeof/IBM_CLOUD_BAREMETAL_BUG_COVERAGE.md` |
| Suite | `suites/tentacle/nvmeof/tier-2_nvmeof_ibm_cloud_baremetal_bug_coverage.yaml` |
| Conf | `conf/tentacle/nvmeof/ceph_nvmeof_ha_cluster_4nodes.yaml` |
| Module | `tests/nvmeof/test_ceph_nvmeof_host_maintenance_io.py` |
| Module | `tests/nvmeof/test_ceph_nvmeof_host_remove_stability.py` |
| Module | `tests/nvmeof/test_ceph_nvmeof_anagrp_integrity.py` |
| Module | `tests/nvmeof/test_ceph_nvmeof_gw_scale_deleting.py` |
| Module | `tests/nvmeof/test_ceph_nvmeof_failover_slo.py` |
| Module | `tests/nvmeof/test_ceph_nvmeof_spec_no_plaintext_keys.py` |
| Module | `tests/nvmeof/test_ceph_nvmeof_daemon_types_upgrade.py` |
| Module | `tests/nvmeof/test_ceph_nvmeof_initiator_reboot_autoconnect.py` |

Companions (deeper coverage already elsewhere): HA suites, 8-GW loadbalancing, encryption/DHCHAP E2E, full version-to-version upgrade suites.

---

## Recommended run order / risk notes

1. **Deploy** (prereq → cephadm → clients → 4-GW nvmeof) — abort-on-fail  
2. **Spec plaintext keys** — cheap, non-destructive  
3. **ANA integrity (+ churn)** — before placement mutations  
4. **Initiator reboot autoconnect** — persistent / discovery.conf / nvmf-autoconnect; before host maintenance  
5. **Pure maintenance under IO** (blocker) — needs healthy 4-GW set  
6. **Maintenance + nvme-gw disable/enable** — uses alternate GW node  
7. **Host remove** then **keep-connections** — peer-restart sensitive; abort-on-fail false  
8. **Scale-down DELETING** — **mutates placement (drops node9)**; keep near end  
9. **Failover SLO** — expects remaining 3 GWs after scale-down  
10. **daemon-types probe** — stop upgrade if it starts  

**Risks / env needs**

- IBM cloud BM inventory + realistic maintenance latency; lab VMs may under-reproduce 15819.  
- `keep-connections` requires GW CLI that supports the flag; older builds will surface as host.delete failures.  
- Full PSK-rotation scenario for 17223 still needs encryption/DHCHAP companion (this suite covers keep-connections + crash storm only).  
- `fail_on_slo: true` for 15815 should be enabled only after baselining BM timings.  
- 15679 acceptance ≠ full staggered upgrade — use upgrade companion suites for image-to-image.

---

## Suggested gates

- **Nightly IBM cloud BM:** this suite  
- **Release:** plus HA depth / LB 8-GW / encryption E2E / full upgrade companions
