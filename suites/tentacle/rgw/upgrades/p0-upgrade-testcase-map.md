# P0 RGW upgrade suites

Setup only. No functional testcases. Single site is tier-1. Multisite + archive is tier-2.

| Folder | Suite | Release | Path |
|---|---|---|---|
| `tier-1/` | `tier1_upgrade_ss_ec_ssl_7x(ga)_9x.yaml` | Reef | 7.1 GA to 9.x |
| `tier-1/` | `tier1_upgrade_ss_ec_ssl_8x(ga)_9x.yaml` | Squid | 8.1 GA to 9.x |
| `tier-1/` | `tier1_upgrade_ss_ec_ssl_9.1x(ga)_9x.yaml` | Tentacle | 9.1 GA to 9.x |
| `tier-2/` | `tier2_upgrade_ms_archive_ec_ssl_7x(ga)_9x.yaml` | Reef | 7.1 GA to 9.x |
| `tier-2/` | `tier2_upgrade_ms_archive_ec_ssl_8x(ga)_9x.yaml` | Squid | 8.1 GA to 9.x |
| `tier-2/` | `tier2_upgrade_ms_archive_ec_ssl_9.1x(ga)_9x.yaml` | Tentacle | 9.1 GA to 9.x |

Single-site feature tests are 5 before the upgrade and the same 5 after it. A config used in one single-site suite is not used in the other two. s3cmd, M buckets with N objects, multipart put or delete, and Swift are in every single-site suite. The fifth case is ordered listing on 7.1, vault SSE-S3 on 8.1, and STS rename on 9.1. Dynamic resharding, lifecycle expiration, compression, and NFS create are not in these suites at this cap.

| Suite | The 5 feature tests |
|---|---|
| 7.1 | s3cmd GET `test_get_s3cmd.yaml`, M buckets etag, multipart put `test_Mbuckets_with_Nobjects_multipart.yaml`, Swift storage policy, ordered listing |
| 8.1 | s3cmd RGW log `test_rgw_log_details.yaml`, M buckets download, multipart delete `test_Mbuckets_with_Nobjects_delete.yaml`, Swift basic ops, vault SSE-S3 per bucket |
| 9.1 | s3cmd header size, M buckets `test_Mbuckets_with_Nobjects.yaml`, multipart failed parts, Swift versioning, STS rename |

Vault agent install sits next to the 8.1 encryption test and is not one of the 5. Cases that were not in an upgrade suite came from `tier-1_rgw.yaml`, `tier-1_rgw_and_lc.yaml`, and `tier-2_rgw_test_using_s3cmd.yaml`.

Each suite runs, in order:

1. Prereq
2. Deploy the same way as the other upgrade suites: `cephadm bootstrap` with `rhcs-version`, `release: released`, and `platform: rhel-9`, then hosts, mgr, mon, osd, the EC 2+2 pools, and the SSL RGW spec. Single site has 3 RGW daemons. Multisite has 6 per site (3 `rgw_io` + 3 `rgw_sync`).
3. Monitoring stack (prometheus, grafana, alertmanager, node-exporter, crash). Bootstrap still uses `skip-monitoring-stack: true`, then this step applies the spec. On tier-2 it is applied on primary, secondary, and archive.
4. Client, then the SSL CA copy and HAProxy in front of the RGW port
5. Multisite, tier-2 only: realm and zone setup, realm info, non-tenanted user create
6. Upgrade
7. CA copy again, then `ceph versions`

Cluster conf:

- Tier-1: `conf/tentacle/rgw/tier-0_rgw.yaml`
- Tier-2: `conf/tentacle/rgw/3-way-archive-ec-profile-2+2@4nodes.yaml`
