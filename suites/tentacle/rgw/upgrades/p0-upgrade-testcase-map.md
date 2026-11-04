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

Single-site feature tests are 5 before the upgrade and the same 5 after it. A config used in one single-site suite is not used in the other two. Swift is in every single-site suite. s3cmd cases that create the bucket with s3cmd are not used on these SSL suites: that helper curls `ip:443` without HTTPS, treats the empty reply as unreachable, and returns before the bucket exists. The s3cmd cases kept here create the bucket with boto first.

| Suite | The 5 feature tests |
|---|---|
| 7.1 | s3cmd delete lifecycle `test_deletelifecycle.yaml`, M buckets etag, multipart put `test_Mbuckets_with_Nobjects_multipart.yaml`, Swift storage policy, ordered listing |
| 8.1 | M buckets AWS4 `test_Mbuckets_with_Nobjects_aws4.yaml`, M buckets download, multipart delete `test_Mbuckets_with_Nobjects_delete.yaml`, Swift basic ops, vault SSE-S3 per bucket |
| 9.1 | s3cmd delete marker `test_multiple_delete_marker_check.yaml`, M buckets `test_Mbuckets_with_Nobjects.yaml`, GetObjectAttributes multipart, Swift versioning, STS rename |

Vault agent install sits next to the 8.1 encryption test and is not one of the 5. Multisite user create SSHes to the first zone endpoint, node2. That node has the rgw role so `ceph-common` and the admin keyring are present for `radosgw-admin sync status`.

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
