# CephCI ODF/Rook-like defaults — design document

## 1. Objective

Make **standalone CephCI (cephadm) clusters** able to look and behave closer to
**OpenShift Data Foundation (ODF) / Rook** clusters, so CI can catch ODF-relevant
regressions **without changing default CephCI behavior**.

| Goal | Why |
|------|-----|
| Opt-in ODF-like Ceph config at bootstrap | Seed OSDMap ratios and daemon defaults the way Rook’s `rook-config-override` does |
| Opt-in v2-only mon endpoints | Match ODF `requireMsgr2`-style monmaps (`v2:IP:3300` only) |
| Compact Rook-aligned topology | Racks + `ssd` class + `.mgr_rack_ssd` via `deploy_rook_defaults_ceph.py` (not zone `apply-odf-topology`) |
| Keep CephFS / RBD / SMB tests working | Kernel CephFS mounts break on v2-only clusters unless mounts use msgr2 correctly |
| Zero impact when flags are off | Existing suites and smoke runs stay unchanged |

---

## 2. Solution in CephCI (architecture)

### 2.1 Opt-in flags (`run.py` `--custom-config`)

```bash
--custom-config apply-odf-defaults=true    # full profile + v2-only set-addrs
--custom-config verify-odf-defaults=true   # assert config/osd/mon dump after deploy

# DO NOT USE:
# --custom-config apply-odf-topology=true  # legacy zone path; superseded by
#                                          # deploy_rook_defaults_ceph.py
```

Suite YAML keys win over profile keys when both set the same option.

### 2.2 Code map

| Component | Path | Responsibility |
|-----------|------|----------------|
| Profile YAML | `conf/tentacle/rook/odf_rook_defaults.yaml` | Declared ODF-like defaults |
| Defaults helper | `utility/odf_defaults.py` | Merge into bootstrap; `set-addrs`; verify; kernel mount helpers |
| Topology helper | `utility/odf_topology.py` | **Do-not-use** legacy zones / `odf-*` rules / container limits / label SSD |
| Bootstrap | `ceph/ceph_admin/bootstrap.py` | Merge profile into `--config`; post-bootstrap `set-addrs` |
| Deploy | `tests/ceph_installer/test_cephadm.py` | Re-apply `set-addrs` after mon steps / full deploy; optional verify |
| Mon apply | `tests/cephadm/test_mon.py` | `set-addrs` after mon `apply` when defaults flag set |
| Topology entry | `tests/ceph_installer/apply_odf_topology.py` | **Do-not-use** standalone zone topology wrapper |
| Rook-aligned deploy | `tests/ceph_installer/deploy_rook_defaults_ceph.py` | Post-bootstrap hosts+racks, mon/mgr/OSD `ssd`, `.mgr_rack_ssd` |
| Sample suite | `suites/tentacle/rook/deploy_rook_defaults_ceph.yaml` | Vanilla bootstrap + the module above |
| Unit tests | `unittests/utility/test_odf_*.py`, `unittests/ceph_installer/` | Profile merge, set-addrs, topology, rack/volume helpers |

**Design choice:** `ceph mon set-addrs` is **not** in library `ceph/ceph_admin/mon.py`.
It runs only when `apply-odf-defaults=true`, at bootstrap / deploy / mon-apply hooks.

### 2.3 Lifecycle (what runs when)

```text
run.py  -c apply-odf-defaults=true  [-c verify-odf-defaults=true]
   │
   ├─ bootstrap
   │     ├─ merge odf_rook_defaults.yaml → args.config → cephadm bootstrap --config
   │     └─ after success: ceph mon set-addrs <mon> '[v2:<ip>:3300/0]'   (first mon)
   │
   ├─ test_cephadm deploy (or deploy_rook_defaults_ceph.py for rack+ssd)
   │     ├─ after each mon apply step: set-addrs again (all current mons)
   │     ├─ after full deploy: set-addrs again
   │     └─ if verify-odf-defaults: compare config dump + osd dump ratios + v2-only monmap
   │
   │  (apply-odf-topology: DO-NOT-USE — legacy zones; do not enable)
   │
   └─ later tests (CephFS/RBD/SMB/RADOS)
         └─ kernel CephFS mounts consult: ceph config get mon ms_bind_msgr1
              false → device :3300 + ms_mode=crc
              true  → legacy bare IP / :6789 behavior
```

### 2.4 Rook-aligned deploy suite (racks + `default~ssd`)

[`suites/tentacle/rook/deploy_rook_defaults_ceph.yaml`](../suites/tentacle/rook/deploy_rook_defaults_ceph.yaml)
uses **vanilla bootstrap** (no Rook keys in suite args). Cluster conf:
[`conf/tentacle/common/3node-1client.yaml`](../conf/tentacle/common/3node-1client.yaml).

```text
install_prereq → test_cephadm bootstrap (mon-ip node1)
              → deploy_rook_defaults_ceph.py
              → optional test_client
```

Pass `-c apply-odf-defaults=true` on `run.py` for the YAML profile + first-mon `set-addrs`.
Omit `-c` for topology-only (stock Ceph config, still racks + ssd class).

This module does **not** re-check [`odf_rook_defaults.yaml`](../conf/tentacle/rook/odf_rook_defaults.yaml)
against `ceph config dump`. That profile is merged at **bootstrap**; use
`-c verify-odf-defaults=true` on the bootstrap/`test_cephadm` step if you want
config-dump vs YAML. The module verifies crush topology it created (racks, `ssd`, `.mgr`).

The module:

1. Fails if an **osd** role node has no volumes, or if OSDs already exist (no crush-move).
2. Applies host specs: labels from roles; OSD roles get `location: {root: default, rack: rackN}`.
3. `ceph orch apply mon/mgr` by label; `set-addrs` again when the defaults flag is on.
4. OSD DriveGroup `crush_device_class: ssd` so the shadow root **`default~ssd`** exists
   (not `default~ssd_class`). Fallback: `ceph osd crush set-device-class ssd`.
5. `ceph osd crush rule create-replicated .mgr_rack_ssd default rack ssd`
   → `take default~ssd` + `chooseleaf type rack`. Binds `.mgr`. Leaves `replicated_rule` (host) alone.

Do **not** use `apply-odf-topology` (do-not-use). That flag enables legacy **zones**
and conflicts with this rack-based suite.

---

## 3. Default configuration applied

### 3.1 Full profile (`apply-odf-defaults=true`)

Source: [`conf/tentacle/rook/odf_rook_defaults.yaml`](../conf/tentacle/rook/odf_rook_defaults.yaml)

#### Global

| Key | Value | Purpose / effect |
|-----|-------|------------------|
| `mon_osd_full_ratio` | `0.85` | Seeded into **OSDMap** at cluster create (ODF-like full threshold; Ceph default ~0.95) |
| `mon_osd_backfillfull_ratio` | `0.80` | OSDMap backfill-full threshold |
| `mon_osd_nearfull_ratio` | `0.75` | OSDMap nearfull threshold |
| `mon_max_pg_per_osd` | `1000` | PG-per-OSD ceiling (some ODF training samples use 600; CephCI profile uses 1000) |
| `mon_target_pg_per_osd` | `200` | Target PG density |
| `mon_pg_warn_max_object_skew` | `0` | Soften skew warnings |
| `mon_data_avail_warn` | `15` | Mon disk avail warn % |
| `bdev_flock_retry` | `20` | Device flock retries |
| `bluestore_prefer_deferred_size_hdd` | `0` | Bluestore deferred write tuning |
| `bluestore_slow_ops_warn_lifetime` | `0` | Slow-ops warning lifetime |
| `ms_bind_msgr1` | `false` | Daemons do not bind msgr1 (**mons often still keep v1 in monmap until set-addrs** — [tracker #70457](https://tracker.ceph.com/issues/70457)) |
| `rbd_default_map_options` | `ms_mode=prefer-crc` | Default **krbd** map options (prefer msgr2 crc; can still fall back) |

#### OSD

| Key | Value | Purpose |
|-----|-------|---------|
| `osd_memory_target_cgroup_limit_ratio` | `0.8` | OSD memory target vs cgroup limit (ODF-like) |

#### MDS

| Key | Value | Purpose |
|-----|-------|---------|
| `mds_cache_memory_limit` | `3221225472` (~3 GiB) | MDS cache cap closer to ODF |

### 3.2 Post-deploy monmap (same flag)

For each mon:

```bash
ceph mon set-addrs <name> '[v2:<ip>:3300/0]'
```

**Effect:** Public monmap becomes **v2-only** (no `:6789` / `v1:`). This is the
ODF-like “require msgr2” mon endpoint shape.

### 3.3 Topology-only bootstrap (`apply-odf-topology=true`) — **DO NOT USE**

Legacy. Prefer `-c apply-odf-defaults=true` for msgr2/RBD options in the full profile.

If somehow set alone (unsupported), it historically injected only:

```text
global.ms_bind_msgr1 = false
global.rbd_default_map_options = ms_mode=prefer-crc
```

### 3.4 Topology post-OSD (`apply-odf-topology=true`) — **DO NOT USE**

**Superseded** by [`deploy_rook_defaults_ceph.py`](../tests/ceph_installer/deploy_rook_defaults_ceph.py)
(compact `root→rack→host→osd` + DriveGroup `ssd` + `.mgr_rack_ssd`).

Do **not** enable this flag on new or existing suites. Code remains for legacy
experiments only and logs a warning when invoked.

Historical steps (for archaeology): `zones`, `crush_rules`, `container_limits`, `ssd_class`

| Step | What was applied | Effect / caveats |
|------|------------------|------------------|
| `zones` | region/zone CRUSH buckets; move first 3 hosts into `zone-a/b/c` | Skipped if fewer than 3 hosts; **conflicts with rack deploy** |
| `crush_rules` | Creates rules `odf-block`, `odf-fs-meta`, `odf-fs-data` (failure domain `zone`) | **Does not auto-rebind existing pools** |
| `container_limits` | Orch specs: OSD 2 CPU / 5g; MON 1 CPU / 2g; MDS 2 CPU / 6g | May OOM on small CephCI VMs |
| `ssd_class` | `ceph osd crush set-device-class ssd <all osds>` | Label only; prefer DriveGroup `crush_device_class` |

---

## 4. Effects of applying these defaults

### 4.1 Cluster behavior

| Area | Effect |
|------|--------|
| Capacity warnings / full handling | Nearfull/backfillfull/full trip earlier (0.75 / 0.80 / 0.85) like ODF |
| PG planning | Higher `mon_max_pg_per_osd` / target density vs stock CephCI |
| Messenger | New binds prefer msgr2 only; after set-addrs, **clients must use v2 (:3300)** to reach mons |
| RBD kernel map | Gets `ms_mode=prefer-crc` from central config |
| MDS | Larger cache limit |
| Topology | Use `deploy_rook_defaults_ceph.py` (racks + ssd). Do **not** use `apply-odf-topology` |

### 4.2 CephFS — the critical side effect

| Client | Before ODF flags | After ODF flags (without mount fix) | After mount fix |
|--------|------------------|--------------------------------------|-----------------|
| **ceph-fuse** | Works | Generally still works (userspace msgr2) | Works |
| **Kernel CephFS** | Bare `IP:/` → **:6789** | Connects to dead/missing v1 → **“no mds is up”**, corrupt mdsmap, dmesg `no match of type 1 in addrvec` | Uses **`IP:3300` + `ms_mode=crc`** → mounts succeed |

Why `prefer-crc` alone is not enough for CephFS kernel:

- That option is primarily for **krbd** (`rbd map`).
- CephFS kernel mount still needs an explicit **port** and **`ms_mode=crc`** (no v1
  fallback) when monmap is v2-only.
- Bare `mount -t ceph IP:/` defaults to port **6789** ([mount.ceph(8)](https://docs.ceph.com/en/latest/man/8/mount.ceph/)).

### 4.3 When flags are off

No profile merge, no set-addrs, no topology, no mount auto-rewrite from
`ms_bind_msgr1` (helper returns false → legacy paths). Default CephCI unchanged.

---

## 5. Fixes applied (CephFS / clients)

### 5.1 Decision policy

```text
ceph config get mon ms_bind_msgr1
  → false / 0 / no  ⇒ use_msgr2
  → else            ⇒ legacy

Additionally (V1 kernel_mount only):
  mon dump is v2-only (has v2, no v1) ⇒ use_msgr2
  (covers ODF where ms_bind_msgr1 may stay true)

If caller already sets ms_mode=… (e.g. ms_mode=legacy in option matrix):
  ⇒ do not auto-rewrite port / do not inject ms_mode=crc
```

Helpers in `utility/odf_defaults.py`:

- `is_msgr1_disabled(client)`
- `format_mons_for_kernel_mount(...)` — bare IP / list / `v2:IP:3300/0` → `IP:3300` when msgr2
- `kernel_ms_mode_opt(...)` — append `,ms_mode=crc` if needed

### 5.2 Call sites fixed

| Site | Fix |
|------|-----|
| `tests/cephfs/cephfs_utilsV1.py` `kernel_mount` | Auto `:3300` + `ms_mode=crc`; respect explicit `ms_mode` |
| `tests/cephfs/cephfs_utilsV1.py` `validate_fs_info` | Expect FS mon ports `:3300` when msgr1 disabled, else `:6789` |
| `tests/cephfs/cephfs_utils.py` (V0) | Same helpers |
| `utility/utils.py` `kernel_mount` | Same helpers (replaces hardcoded `:6789` behavior) |
| `tests/cephfs/no_recover_session_mount.py` | Direct mount updated |
| `tests/cephfs/BUG-1798719.py` | Direct mount updated |
| `tests/cephfs/fs_kernel_mount_options.py` | Default/direct mounts updated; **legacy ms_mode left explicit** |
| `tests/smb/smb_operations.py` `samba_kernel_mount` | Same policy |
| `ceph/rados/core_workflows.py` `admin@.fs=/` | Adds `ms_mode=crc` + `mon_addr=IP:3300/...` when msgr1 disabled |
| `tests/rados/test_bug_fixes.py` | Normalizes mon `public_addr` + `ms_mode=crc` when needed |

### 5.3 What those fixes achieve

- Kernel CephFS works on ODF-flagged CephCI clusters.
- `validate_fs_info` no longer fails by comparing `:6789` expectations to `:3300` FS info.
- Bypass mounts that skipped V1 no longer regress under msgr2-only monmaps.
- Option-matrix tests that **intentionally** pass `ms_mode=legacy` are not silently rewritten to crc.

---

## 6. Configuration / behavior still left to apply (gaps)

### 6.1 Ceph config often present in real ODF but not in CephCI profile

| Item | Status | Notes |
|------|--------|------|
| Exact ODF `mon_max_pg_per_osd` (sometimes 600) | Profile uses **1000** | Align if parity with a specific ODF version is required |
| `osd_pool_default_size` / `min_size` | **Not applied** | Common in rook-config-override examples (e.g. size 2 / min 1 for compact clusters) |
| Full Rook operator / CSI / StorageCluster CR semantics | **N/A** | CephCI is bare cephadm, not Rook |
| Network encryption / msgr2 secure mode | **Not applied** | Only crc/prefer-crc style options today |
| Additional ODF ConfigMap knobs (version-specific) | **Not inventoried end-to-end** | Profile is a curated subset |

### 6.2 Mon / messenger parity with ODF

| Item | CephCI today | Typical ODF | Left to decide |
|------|--------------|-------------|----------------|
| `ms_bind_msgr1` | Set **false** via profile | Often left **true**; v2-only via monmap / requireMsgr2 | Whether to drop `ms_bind_msgr1=false` and rely only on `set-addrs` for closer ODF parity |
| Client `ceph.conf` `mon_host` after set-addrs | Not universally rewritten | ODF clients get v2 endpoints from operator | Ensure all clients regenerate minimal conf / mon_host if mounts omit explicit mon list |
| `ms_mode=prefer-crc` for CephFS | Not used (need **crc**) | Kernel CephFS still needs explicit mount opts | Done for known paths; hunt remaining raw mounts |

### 6.3 Topology gaps

| Item | Status |
|------|--------|
| Bind existing pools to `odf-*` crush rules | **Not automatic** — rules created only |
| Zone layout on clusters with fewer than 3 hosts | Zones step **skipped** |
| Container limits on small VMs | Best-effort; may fail/warn |
| SSD class | Label only; no media validation |
| Failure-domain / stretch / arbiter ODF topologies | **Not modeled** beyond simple 3-zone sketch |

### 6.4 Test / mount follow-ups

| Item | Status |
|------|--------|
| Migrate remaining V0 / `utility.utils.kernel_mount` callers to V1 | Recommended |
| Grep for other `mount -t ceph` / hardcoded `:6789` | Ongoing hygiene |
| `fs_kernel_mount_options` + `ms_mode=legacy` on v2-only clusters | Expected to fail or need skip when ODF flags on |
| Fuse-only paths | Generally OK; still verify under verify-odf-defaults |
| Non-CephFS clients (RGW, NVMe-oF, etc.) under v2-only monmap | **Not specifically validated** by this work |

### 6.5 Operational / process

| Item | Status |
|------|--------|
| Default smoke/BVT enabling the flags | Optional — flags are opt-in; decide per suite |
| Document suite authors must pass `-c apply-odf-defaults=true` for ODF parity runs | Needed for adoption |
| Compare live ODF toolbox dumps vs CephCI profile regularly | Recommended when targeting a specific ODF z-stream |

---

## 7. How to run and verify

```bash
# Rook-aligned deploy (racks + ssd class + .mgr rule); profile at bootstrap
python run.py --suite suites/tentacle/rook/deploy_rook_defaults_ceph.yaml \
  --conf conf/tentacle/common/3node-1client.yaml \
  -c apply-odf-defaults=true

# Optional verify of bootstrap profile / v2-only monmap
python run.py ... \
  -c apply-odf-defaults=true \
  -c verify-odf-defaults=true

# DO NOT USE: -c apply-odf-topology=true  (legacy zones; superseded)
```

For ODF interop pipeline runs, always pass `-c verify-odf-defaults=true` together with
`apply-odf-defaults=true` so v2-only monmap (`set-addrs`) regressions fail deploy
instead of passing silently. Attach a reachable Jenkins/magna job URL or log excerpt
showing `Applying v2-only mon addrs` and `ODF defaults verified` in the PR.

Post-deploy checks:

```bash
ceph config get mon ms_bind_msgr1
ceph config get global rbd_default_map_options
ceph osd dump | grep -E 'full_ratio|nearfull|backfillfull'
ceph mon dump                    # expect v2:IP:3300 only
ceph config dump | grep -E 'mon_max_pg|mds_cache|osd_memory_target'
```

Kernel mount sanity (when msgr1 disabled):

```bash
mount -t ceph <ip>:3300:/ <mnt> -o name=admin,secretfile=...,ms_mode=crc
# dmesg should not show persistent :6789 / type 1 addrvec errors
```

Unit tests:

```bash
python -m pytest unittests/utility/test_odf_defaults.py unittests/utility/test_odf_topology.py \
  unittests/ceph_installer/test_deploy_rook_defaults_ceph.py -q
```

---

## 8. Summary table

| Layer | Applied now | Effect | Still left |
|-------|-------------|--------|------------|
| Bootstrap Ceph config | YAML profile (ratios, PG, msgr1 off, RBD prefer-crc, OSD/MDS knobs) | ODF-like OSDMap + daemon defaults | More Rook override keys; size/min_size; version drift |
| Monmap | `set-addrs` → v2-only | Clients must use msgr2 | Optional closer ODF parity (`ms_bind_msgr1` true + monmap only); client conf refresh |
| Topology | Host spec racks + OSD `ssd` class (this suite) | Compact `root→rack→host→osd`; `default~ssd` | Pool↔rule binding for every CephCI pool create; stretch. **`apply-odf-topology` do-not-use** |
| CephFS kernel | `:3300` + `ms_mode=crc` via helpers | Mounts work again | Remaining raw mounts; legacy option-matrix on v2-only; V0→V1 migration |
| Default CephCI (no flags) | Nothing | Unchanged | — |

---

## 9. Takeaway

CephCI can opt into an ODF/Rook-like cluster by merging a curated bootstrap profile,
forcing v2-only mon addresses, and optionally applying topology steps. That combination
disables effective msgr1 access to mons, which broke CephFS **kernel** mounts that
defaulted to `:6789`. The fix is to decide msgr2 from `ceph config get mon ms_bind_msgr1`
(and v2-only monmap in V1) and mount with `:3300` + `ms_mode=crc` across known call
sites. Remaining work is fuller Rook config parity, topology depth, client `mon_host`
hygiene, and hunting any leftover bare kernel mounts—not re-breaking default (non-flag)
CephCI runs.
