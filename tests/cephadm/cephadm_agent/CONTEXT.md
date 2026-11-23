# Cephadm agent scale regression — context

This suite measures cephadm-agent behavior (host add, agent up, mgr failover
retarget, restart, registry auth, monitoring, cert-store size) while the
cluster is busy with **other protocols**. OSD count is intentionally modest.

## Cluster shape

| | DMFG regression | Dedicated pipeline | Smoke |
|---|---|---|---|
| OSD hosts | 40 bring-up + 10 expand (~50) | 100 bring-up + 50 expand (151 cluster nodes) | 5 + 3 |
| OSDs | ~80 + ~20 (2 volumes/host) | 500 + 250 (5 volumes/host) | ~8 + ~4 |
| RGW daemons | 16 | 16 | 2 |
| NFS clusters | 8 (distinct ports) | 8 | 1 |
| MDS | 8 on `ceph fs volume` `agent-scale` | 8 | 2 |
| VMs | 52 | 152 (151 cluster + 1 client) | 10 |

DMFG regression conf: `conf/<release>/cephadm/1admin-51node-1client-agent-scale-ibmc.yaml`
with `suites/<release>/cephadm/tier3-cephadm-agent-scale-regression.yaml`.

Dedicated pipeline conf: `conf/<release>/cephadm/1admin-151node-1client-agent-scale-ibmc.yaml`
with `suites/<release>/cephadm/tier3-cephadm-agent-scale-regression-151.yaml`.
That job is separate so the 152 VMs are not created beside the other DMFG suites.

`allow_fewer_hosts: true` scales the host/OSD targets down when the lab conf
is smaller. TC1 is the only case with `abort-on-fail`. TC7 (firewall) stays
in the tier2 communication suite.

## Guest image

Per-node `image-name` is omitted. The VM image comes from `--inventory`:

- `conf/inventory/ibm-rhel-9.8.yaml` (`rhel-98-server-amd64-kvm`)
- `conf/inventory/ibm-rhel-10.2.yaml` (`rhel-102-server-amd64-kvm`)

RHEL 9.6 is an older image than the builds this suite is compared against
(9.8 and 10.2), so timings from a 9.6 guest would not line up with those runs.

## Timings

`utility/collate_scale_timings.py` is the helper (JSON + HTML, and a two-run
compare). Each TC calls it from `scale_regression_ops.print_report`.

```bash
python3 -m utility.collate_scale_timings /path/to/runA /path/to/runB \
  -o /tmp/compare.html --md /tmp/compare.md
```

Artifacts land in the CephCI log dir (`agent_scale_timings*.json` / `.html`),
which the job already collects.

Optional copy onto storage from the cluster under test: mount that share
first (CephFS `agent-scale` or an NFS export), then set
`scale.timing_share` or `CEPHCI_SCALE_TIMING_SHARE` to the mount path.
`publish_timings_to_share()` copies the timing files there at the end of
each case. The helper does not mount the share itself, so a failed mount
cannot fail the agent measurement.
