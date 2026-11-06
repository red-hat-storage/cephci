# P0 RGW upgrade suites

Single site is tier-1. Two-site multisite is tier-2.

| Folder | Suite | Release | Path |
|---|---|---|---|
| `tier-1/` | `tier1_upgrade_ss_ec_ssl_7x(ga)_9x.yaml` | Reef | 7.1 GA to 9.x |
| `tier-1/` | `tier1_upgrade_ss_ec_ssl_8x(ga)_9x.yaml` | Squid | 8.1 GA to 9.x |
| `tier-1/` | `tier1_upgrade_ss_ec_ssl_9.1x(ga)_9x.yaml` | Tentacle | 9.1 GA to 9.x |
| `tier-2/` | `tier2_upgrade_ms_archive_ec_ssl_7x(ga)_9x.yaml` | Reef | 7.1 GA to 9.x |
| `tier-2/` | `tier2_upgrade_ms_archive_ec_ssl_8x(ga)_9x.yaml` | Squid | 8.1 GA to 9.x |
| `tier-2/` | `tier2_upgrade_ms_archive_ec_ssl_9.1x(ga)_9x.yaml` | Tentacle | 9.1 GA to 9.x |

Single-site feature tests are 5 before the upgrade and the same 5 after it. A config used in one single-site suite is not used in the other two. Swift is in every single-site suite. s3cmd cases that create the bucket with s3cmd are not used on these SSL suites: that helper curls `ip:443` without HTTPS, treats the empty reply as unreachable, and returns before the bucket exists. The 7.1 s3cmd case creates the bucket with boto first. The 9.1 s3cmd case only checks RGW log files, which is the path that passed on the 8.1 SSL upgrade.

| Suite | The 5 feature tests |
|---|---|
| 7.1 | s3cmd delete lifecycle `test_deletelifecycle.yaml`, M buckets etag, multipart put `test_Mbuckets_with_Nobjects_multipart.yaml`, Swift storage policy, ordered listing |
| 8.1 | M buckets AWS4 `test_Mbuckets_with_Nobjects_aws4.yaml`, M buckets download, multipart delete `test_Mbuckets_with_Nobjects_delete.yaml`, Swift basic ops, vault SSE-S3 per bucket |
| 9.1 | s3cmd rgw log details `test_rgw_log_details.yaml`, M buckets `test_Mbuckets_with_Nobjects.yaml`, GetObjectAttributes multipart, Swift versioning, STS rename |

Vault agent install sits next to the 8.1 single-site encryption test and is not one of the 5. Two-site user create copies the user file to secondary. Node2 is the first zone endpoint and has the rgw role, so `ceph-common` and the admin keyring are present for `radosgw-admin sync status`.

Tier-2 is two-site multisite, primary and secondary. Swift is not required. Each suite has 5 feature tests before the upgrade and the same 5 after it, taken from the upgrade canvas where that case can run twice on SSL. A config used in one tier-2 suite is not used in the other two. Object tests check IO on secondary.

| Suite | The 5 feature tests |
|---|---|
| 7.1 | s3cmd delete lifecycle, M buckets `test_Mbuckets_with_Nobjects.yaml`, multipart put, ordered listing, compression |
| 8.1 | M buckets `test_Mbuckets.yaml`, object delete, unordered listing, bucket versioning, M buckets download |
| 9.1 | pseudo-directory ordered listing, M buckets AWS4, versioned object copy, ordered listing of versioned buckets, delete multiple buckets |

Each suite runs, in order:

1. Prereq
2. Deploy the same way as the other upgrade suites: `cephadm bootstrap` with `rhcs-version`, `release: released`, and `platform: rhel-9`, then hosts, mgr, mon, osd, the EC 2+2 pools, and the SSL RGW spec. Single site has 3 RGW daemons on port 443. Two-site multisite has 6 per site: 3 `rgw_io` on port 443 and 3 `rgw_sync` on port 444, both on node2, node3, and node4. Sync uses 444 so it does not share the dashboard port. Reef 7.1 does not accept `disable_multisite_sync_traffic` in the RGW spec, so that suite sets `rgw_run_sync_thread false` on the io service instead.
3. Monitoring stack (prometheus, grafana, alertmanager, node-exporter, crash). Bootstrap still uses `skip-monitoring-stack: true`, then this step applies the spec. On tier-2 it is applied on primary and secondary.
4. Client, then the SSL CA copy and HAProxy in front of the RGW port
5. Multisite, tier-2 only: realm and zone setup for primary and secondary, realm info, non-tenanted user create with the user file copied to secondary
6. The 5 feature tests
7. Upgrade
8. CA copy again, then `ceph versions`, then the same 5 feature tests

Cluster conf:

- Tier-1: `conf/tentacle/rgw/tier-0_rgw.yaml`
- Tier-2: `conf/tentacle/rgw/2-site-ec-profile-2+2@4nodes.yaml`
