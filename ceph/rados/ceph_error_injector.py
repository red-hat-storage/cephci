"""
ceph_error_injector.py - Unified Ceph Error Injection Framework

This module provides a structured, reusable class for injecting errors into a
running Ceph cluster during thrash/stress testing. It catalogs all known Ceph
inject debug variables and admin socket injection commands, provides methods to
enable/disable them on demand, and automatically tracks active injections for
guaranteed cleanup.

==============================================================================
USAGE GUIDE
==============================================================================

1. Basic Usage (single injection):

    from ceph.rados.ceph_error_injector import CephErrorInjector

    injector = CephErrorInjector(rados_obj=rados_obj)
    injector.inject_config("ms_inject_delay_probability", 0.1)
    # ... run workload ...
    injector.cleanup_all()

2. Context Manager (automatic cleanup):

    with CephErrorInjector(rados_obj=rados_obj) as injector:
        injector.inject_config("bluestore_debug_inject_read_err", True)
        injector.inject_ec_write_error(osd_id=3, pool="ec-pool")
        # ... run workload ...
    # cleanup_all() called automatically on exit

3. Batch Injection:

    injector.inject_config_batch({
        "osd_debug_inject_dispatch_delay_probability": 0.2,
        "osd_debug_inject_dispatch_delay_duration": 3,
    })

4. Chaos Profiles (predefined combinations):

    injector.apply_profile("network_chaos")
    injector.apply_profile("storage_corruption",
        bluestore_debug_inject_csum_err_probability=0.05)

5. Suite YAML Configuration:

    Add an `error_injection` block under `config` in your suite YAML.
    In your test code, call apply_from_config() to apply it:

        injector = CephErrorInjector(rados_obj=rados_obj)
        injector.apply_from_config(config.get("error_injection", {}))
        # ... run workload ...
        injector.cleanup_all()

    Example A -- Profile only (applies all configs in the profile):

        config:
          error_injection:
            profile: network_chaos

    Example B -- Profile with overrides:

        config:
          error_injection:
            profile: network_chaos
            profile_overrides:
              ms_inject_delay_probability: 0.2
              ms_inject_delay_max: 10

    Example C -- Individual configs (no profile):

        config:
          error_injection:
            configs:
              bluestore_debug_inject_read_err: true
              osd_debug_inject_dispatch_delay_probability: 0.1
              osd_debug_inject_dispatch_delay_duration: 3
              mds_inject_health_dummy: true

    Example D -- Profile + extra configs + EC write errors:

        config:
          error_injection:
            profile: storage_corruption
            configs:
              osd_debug_inject_dispatch_delay_probability: 0.05
            ec_write_errors:
              - osd_id: 0
                pool: ec-pool-1
                shard: 2
                duration: 500
              - osd_id: 1
                pool: ec-pool-1
                shard: 2
                duration: 500

    Example E -- Full config with admin socket commands:

        config:
          error_injection:
            profile: mon_instability
            profile_overrides:
              mon_inject_transaction_delay_probability: 0.2
            configs:
              bluestore_debug_inject_read_err: true
            ec_write_errors:
              - osd_id: 0
                pool: ec-pool-1
                shard: 2
                duration: 1000
            ec_read_errors:
              - osd_id: 2
                pool: ec-pool-1
                shard: 2
                duration: 500
            admin_socket:
              - command: injectfull
                osd_id: 3
                full_type: nearfull
                count: 1
              - command: injectdataerr
                osd_id: 1
                pool: ec-pool-1
                objname: test-object-1
              - command: bluefs_read_zeros
                osd_id: 4
              - command: injectargs
                daemon_type: osd
                daemon_id: "*"
                args:
                  ms_type: simple

    Suite YAML Schema Reference:

        error_injection:
          profile: <profile_name>          # optional, predefined chaos profile
            # Available profiles: network_chaos, network_latency,
            #   storage_corruption, bdev_crash, mon_instability,
            #   mon_sync_delay, mds_chaos, mds_corruption,
            #   rgw_sync_errors, rgw_latency, rgw_multipart_errors,
            #   osd_dispatch_delays, osd_map_corruption,
            #   client_faults, heartbeat_failure,
            #   bluestore_full_corruption, rgw_olh_errors,
            #   mon_election_stress, client_session_stress,
            #   objecter_faults, rgw_full_sync_chaos,
            #   multi_layer_chaos
          profile_overrides:               # optional, override profile values
            <config_key>: <value>
          configs:                          # optional, individual config keys
            <config_key>: <value>
          ec_write_errors:                  # optional, list of EC write specs
            - osd_id: <int>                #   required
              pool: <str>                  #   required
              obj: <str>                   #   default: "*"
              shard: <int>                 #   default: 2
              err_type: <int>              #   default: 1
              skip: <int>                  #   default: 0
              duration: <int>              #   default: 1000
          ec_read_errors:                   # optional, same schema as above
            - osd_id: <int>
              pool: <str>
          admin_socket:                     # optional, admin socket commands
            - command: injectfull
              osd_id: <int>
              full_type: <str>             # "full", "nearfull", "backfillfull"
              count: <int>                 # default: 1
            - command: injectdataerr
              osd_id: <int>
              pool: <str>
              objname: <str>
              shard: <int>                 # optional, for EC pools
            - command: injectmdataerr
              osd_id: <int>
              pool: <str>
              objname: <str>
              shard: <int>                 # optional
            - command: bluefs_read_zeros
              osd_id: <int>
            - command: injectargs
              daemon_type: <str>           # osd, mon, mds, mgr
              daemon_id: <str>             # e.g., "0", "*" for all
              args:
                <key>: <value>

    All sections are optional. Use any combination. cleanup_all()
    in the test's finally block removes everything automatically.

==============================================================================
INJECT VARIABLE REFERENCE
==============================================================================

All variables verified from live cluster via `ceph config help`. Every variable
below is runtime-updatable EXCEPT `client_debug_inject_features`.

--- MESSENGER / NETWORK (section: global) ---
heartbeat_inject_failure        int    0     Skip sending heartbeats (triggers peer-down)
ms_inject_socket_failures       uint   0     Inject socket failure every Nth operation
ms_inject_delay_type            str    ""    Daemon type to inject delays for (osd/mon/mds)
ms_inject_delay_max             float  1.0   Max injected message delay (seconds)
ms_inject_delay_probability     float  0.0   Probability of delaying any message (0.0-1.0)
ms_inject_internal_delays       float  0.0   Internal dispatch delays (seconds)
ms_inject_network_congestion    uint   0     Simulate congestion, stuck N operations

--- MON (section: mon) ---
mon_scrub_inject_crc_mismatch           float  0.0   Probability of CRC mismatch during scrub
mon_scrub_inject_missing_keys           float  0.0   Probability of missing keys during scrub
mon_inject_sync_get_chunk_delay         float  0.0   Delay (seconds) fetching sync chunks
mon_inject_transaction_delay_max        float  10.0  Max paxos transaction delay (seconds)
mon_inject_transaction_delay_probability float  0.0  Probability of delaying paxos transaction
mon_inject_pg_merge_bounce_probability  float  0.0   Probability of bouncing PG merge proposals

--- OBJECTER (section: global) ---
objecter_inject_no_watch_ping           bool   false  Skip watch ping (triggers timeout)
objecter_debug_inject_relock_delay      bool   false  Add delay to relock operations

--- OSD (section: osd) ---
osd_debug_inject_dispatch_delay_probability  float  0.0   Probability of dispatch delay
osd_debug_inject_dispatch_delay_duration     float  0.1   Duration of dispatch delay (seconds)
osd_debug_inject_copyfrom_error              bool   false  Inject error in copy-from ops
osd_inject_bad_map_crc_probability           float  0.0   Probability of bad OSD map CRC
osd_inject_failure_on_pg_removal             bool   false  Force failure during PG removal

--- BLUESTORE / BLOCK DEVICE (section: osd, except bluestore_debug_inject_read_err=global) ---
bdev_inject_crash                                    int    0     Crash on Nth bdev operation
bdev_inject_crash_flush_delay                        int    2     Delay before crash during flush
bluestore_debug_inject_allocation_from_file_failure  float  0.0   Prob of alloc-from-file err
bluestore_debug_inject_read_err                      bool   false  Inject read errors (global)
bluestore_debug_inject_csum_err_probability          float  0.0   Probability of checksum error

--- FILESTORE (section: osd) ---
filestore_debug_inject_read_err  bool  false  Inject read errors
filestore_inject_stall           int   0      Stall operations (seconds)

--- RGW (section: client.rgw) ---
rgw_mp_lock_inject_delay                           int    0     Delay after multipart lock (seconds)
rgw_mp_lock_inject_renewal_error                   int    0     Error code for lock renewal
rgw_sync_data_inject_err_probability               float  0.0   Probability of data sync error
rgw_sync_meta_inject_err_probability               float  0.0   Probability of metadata sync error
rgw_sync_data_full_inject_err_probability          float  0.0   Probability of full data sync error
rgw_inject_delay_sec                               float  0.0   Delay at injection points (seconds)
rgw_inject_delay_pattern                           str    ""    Pattern to match delay points
rgw_data_sync_single_entry_inject_err_probability  float  0.0   Prob of single-entry sync err
rgw_debug_inject_latency_bi_unlink                 uint   0     Latency before BI unlink (s)
rgw_debug_inject_set_olh_err                       uint   0     Error code for OLH set op
rgw_debug_inject_olh_cancel_modification_err       bool   false  Inject OLH cancel mod error
rgw_inject_notify_timeout_probability              float  0.0   Prob of notify timeout (0-1)

--- MDS (section: mds) ---
mds_inject_rename_corrupt_dentry_first      float  0.0   Prob of corrupting dentry in rename
mds_inject_journal_corrupt_dentry_first     float  0.0   Prob of corrupting dentry in journal
mds_inject_health_dummy                     bool   false  Inject fake health warning
mds_inject_skip_replaying_inotable          bool   false  Skip inotable replay on startup
mds_inject_traceless_reply_probability      float  0.0   Probability of traceless replies
mds_inject_migrator_session_race            bool   false  Inject subtree migration race condition

--- CLIENT (section: client) ---
client_debug_inject_tick_delay   secs  0      Delay in tick processing (seconds)
client_debug_inject_features     str   ""     Override feature bits (NOT runtime-updatable)
client_inject_release_failure    bool  false  Fail cap release messages
client_inject_fixed_oldest_tid   bool  false  Fix oldest TID to stale value

--- SIGNAL (section: global) ---
inject_early_sigterm             bool  false  Send SIGTERM early in daemon startup

==============================================================================
ADMIN SOCKET COMMANDS (per-daemon, requires host resolution)
==============================================================================

injectecwriteerr <pool> <obj> <shard:int> [type:int] [when:int] [duration:int]
    Inject write errors for objects in an EC pool.
    - pool: target EC pool name
    - obj: object name or '*' for all
    - shard: shard ID (e.g., 2)
    - type: error type (1=drop sub write, default 1)
    - when: skip first N writes before injecting (default 0)
    - duration: inject for N writes (default 1)

injectecreaderr <pool> <obj> <shard:int> [type:int] [when:int] [duration:int]
    Inject read errors for objects in an EC pool (same params as write).

injectecclearwriteerr <pool> <obj> <shard:int> [type:int]
    Clear previously injected EC write errors.

injectecclearreaderr <pool> <obj> <shard:int> [type:int]
    Clear previously injected EC read errors.

injectdataerr <pool> <objname> [shard:int]
    Inject data corruption on a specific object.

injectmdataerr <pool> <objname> [shard:int]
    Inject metadata corruption on a specific object.

injectfull [type] [count:int]
    Simulate disk full condition.
    - type: "full", "nearfull", "backfillfull" (default "full")
    - count: number of times to inject (default 1)

bluefs debug_inject_read_zeros
    Injects 8K of zeros into the next BlueFS read. Debug only.

injectargs <injected_args>...
    Inject configuration arguments into a running daemon without going
    through the MON config database. Uses `ceph tell` mechanism.
"""

import json
import time

from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)


# =============================================================================
# INJECTION VARIABLE CATALOG
# =============================================================================
# Verified from live cluster via `ceph config ls` and `ceph config help`.
# Each entry: key -> {section, type, default, desc}
# All are runtime-updatable except client_debug_inject_features.

MESSENGER_INJECTIONS = {
    "heartbeat_inject_failure": {
        "section": "global",
        "type": "int",
        "default": 0,
        "desc": "Skip heartbeats for N cycles (DANGEROUS: N=duration not frequency, 0=off)",
    },
    "ms_inject_socket_failures": {
        "section": "global",
        "type": "uint",
        "default": 0,
        "desc": "Inject a socket failure every Nth socket operation",
    },
    "ms_inject_delay_type": {
        "section": "global",
        "type": "str",
        "default": "",
        "desc": "Entity type to inject delays for (osd, mon, mds, etc.)",
    },
    "ms_inject_delay_max": {
        "section": "global",
        "type": "float",
        "default": 1.0,
        "desc": "Max delay to inject on messages (seconds)",
    },
    "ms_inject_delay_probability": {
        "section": "global",
        "type": "float",
        "default": 0.0,
        "desc": "Probability (0.0-1.0) of delaying any given message",
    },
    "ms_inject_internal_delays": {
        "section": "global",
        "type": "float",
        "default": 0.0,
        "desc": "Inject internal delays to induce races (seconds)",
    },
    "ms_inject_network_congestion": {
        "section": "global",
        "type": "uint",
        "default": 0,
        "desc": "Simulate congestion stuck for N ops (DANGEROUS: N=duration)",
    },
}

MON_INJECTIONS = {
    "mon_scrub_inject_crc_mismatch": {
        "section": "mon",
        "type": "float",
        "default": 0.0,
        "desc": "Probability of injecting CRC mismatch during MON scrub",
    },
    "mon_scrub_inject_missing_keys": {
        "section": "mon",
        "type": "float",
        "default": 0.0,
        "desc": "Probability of injecting missing keys during MON scrub",
    },
    "mon_inject_sync_get_chunk_delay": {
        "section": "mon",
        "type": "float",
        "default": 0.0,
        "desc": "Inject delay during MON sync chunk fetch (seconds)",
    },
    "mon_inject_transaction_delay_max": {
        "section": "mon",
        "type": "float",
        "default": 10.0,
        "desc": "Max duration of injected delay in paxos (seconds)",
    },
    "mon_inject_transaction_delay_probability": {
        "section": "mon",
        "type": "float",
        "default": 0.0,
        "desc": "Probability of injecting a delay in paxos transactions",
    },
    "mon_inject_pg_merge_bounce_probability": {
        "section": "mon",
        "type": "float",
        "default": 0.0,
        "desc": "Probability of failing and reverting a pg_num decrement",
    },
}

OBJECTER_INJECTIONS = {
    "objecter_inject_no_watch_ping": {
        "section": "global",
        "type": "bool",
        "default": False,
        "desc": "Skip watch ping, triggers watch timeout on peer",
    },
    "objecter_debug_inject_relock_delay": {
        "section": "global",
        "type": "bool",
        "default": False,
        "desc": "Add delay to relock operations in objecter",
    },
}

OSD_INJECTIONS = {
    "osd_debug_inject_dispatch_delay_probability": {
        "section": "osd",
        "type": "float",
        "default": 0.0,
        "desc": "Probability of adding delay to OSD op dispatch",
    },
    "osd_debug_inject_dispatch_delay_duration": {
        "section": "osd",
        "type": "float",
        "default": 0.1,
        "desc": "Duration of dispatch delay when triggered (seconds)",
    },
    "osd_debug_inject_copyfrom_error": {
        "section": "osd",
        "type": "bool",
        "default": False,
        "desc": "Inject errors in OSD copy-from operations",
    },
    "osd_inject_bad_map_crc_probability": {
        "section": "osd",
        "type": "float",
        "default": 0.0,
        "desc": "Probability of injecting bad CRC in OSD map updates",
    },
    "osd_inject_failure_on_pg_removal": {
        "section": "osd",
        "type": "bool",
        "default": False,
        "desc": "Force failure during PG removal",
    },
}

BLUESTORE_INJECTIONS = {
    "bdev_inject_crash": {
        "section": "osd",
        "type": "int",
        "default": 0,
        "desc": "Crash on Nth bdev operation (simulates SSD firmware crash)",
    },
    "bdev_inject_crash_flush_delay": {
        "section": "osd",
        "type": "int",
        "default": 2,
        "desc": "Delay (seconds) before crash injection during flush",
    },
    "bluestore_debug_inject_allocation_from_file_failure": {
        "section": "osd",
        "type": "float",
        "default": 0.0,
        "desc": "Probability of error restoring allocation map from file",
    },
    "bluestore_debug_inject_read_err": {
        "section": "global",
        "type": "bool",
        "default": False,
        "desc": "Inject read errors in BlueStore (prerequisite for EC inject)",
    },
    "bluestore_debug_inject_csum_err_probability": {
        "section": "osd",
        "type": "float",
        "default": 0.0,
        "desc": "Probability of injecting checksum errors on reads",
    },
}

FILESTORE_INJECTIONS = {
    "filestore_debug_inject_read_err": {
        "section": "osd",
        "type": "bool",
        "default": False,
        "desc": "Inject read errors in FileStore backend",
    },
    "filestore_inject_stall": {
        "section": "osd",
        "type": "int",
        "default": 0,
        "desc": "Stall FileStore operations for N seconds (simulates hung disk)",
    },
}

RGW_INJECTIONS = {
    "rgw_mp_lock_inject_delay": {
        "section": "client.rgw",
        "type": "int",
        "default": 0,
        "desc": "Injected delay after acquiring multipart lock (seconds)",
    },
    "rgw_mp_lock_inject_renewal_error": {
        "section": "client.rgw",
        "type": "int",
        "default": 0,
        "desc": "Injected error code for multipart lock renewal testing",
    },
    "rgw_sync_data_inject_err_probability": {
        "section": "client.rgw",
        "type": "float",
        "default": 0.0,
        "desc": "Probability of error during data sync between zones",
    },
    "rgw_sync_meta_inject_err_probability": {
        "section": "client.rgw",
        "type": "float",
        "default": 0.0,
        "desc": "Probability of error during metadata sync",
    },
    "rgw_sync_data_full_inject_err_probability": {
        "section": "client.rgw",
        "type": "float",
        "default": 0.0,
        "desc": "Probability of error during full data sync",
    },
    "rgw_inject_delay_sec": {
        "section": "client.rgw",
        "type": "float",
        "default": 0.0,
        "desc": "Delay duration at injection points (seconds)",
    },
    "rgw_inject_delay_pattern": {
        "section": "client.rgw",
        "type": "str",
        "default": "",
        "desc": "Pattern to select which delay injection points are activated",
    },
    "rgw_data_sync_single_entry_inject_err_probability": {
        "section": "client.rgw",
        "type": "float",
        "default": 0.0,
        "desc": "Probability of error during single-entry data sync",
    },
    "rgw_debug_inject_latency_bi_unlink": {
        "section": "client.rgw",
        "type": "uint",
        "default": 0,
        "desc": "Latency before bucket index unlink (seconds)",
    },
    "rgw_debug_inject_set_olh_err": {
        "section": "client.rgw",
        "type": "uint",
        "default": 0,
        "desc": "Error code injected on OLH set operation (e.g., errno)",
    },
    "rgw_debug_inject_olh_cancel_modification_err": {
        "section": "client.rgw",
        "type": "bool",
        "default": False,
        "desc": "Inject error when canceling OLH modification",
    },
    "rgw_inject_notify_timeout_probability": {
        "section": "client.rgw",
        "type": "float",
        "default": 0.0,
        "desc": "Probability (0.0-1.0) of ignoring cache notify messages",
    },
}

MDS_INJECTIONS = {
    "mds_inject_rename_corrupt_dentry_first": {
        "section": "mds",
        "type": "float",
        "default": 0.0,
        "desc": "Probability of corrupting first dentry during rename",
    },
    "mds_inject_journal_corrupt_dentry_first": {
        "section": "mds",
        "type": "float",
        "default": 0.0,
        "desc": "Probability of corrupting first dentry in journal",
    },
    "mds_inject_health_dummy": {
        "section": "mds",
        "type": "bool",
        "default": False,
        "desc": "Inject fake health warning into MDS health reports",
    },
    "mds_inject_skip_replaying_inotable": {
        "section": "mds",
        "type": "bool",
        "default": False,
        "desc": "Skip inotable replay on MDS startup",
    },
    "mds_inject_traceless_reply_probability": {
        "section": "mds",
        "type": "float",
        "default": 0.0,
        "desc": "Probability of sending replies without traces",
    },
    "mds_inject_migrator_session_race": {
        "section": "mds",
        "type": "bool",
        "default": False,
        "desc": "Inject race condition in subtree migration",
    },
}

CLIENT_INJECTIONS = {
    "client_debug_inject_tick_delay": {
        "section": "client",
        "type": "secs",
        "default": 0,
        "desc": "Delay in client tick processing (seconds)",
    },
    "client_debug_inject_features": {
        "section": "client",
        "type": "str",
        "default": "",
        "desc": "Override client feature bits (NOT runtime-updatable)",
        "runtime": False,
    },
    "client_inject_release_failure": {
        "section": "client",
        "type": "bool",
        "default": False,
        "desc": "Fail cap release messages (tests cap revocation backoff)",
    },
    "client_inject_fixed_oldest_tid": {
        "section": "client",
        "type": "bool",
        "default": False,
        "desc": "Fix oldest TID to stale value (triggers session blocklist)",
    },
}

SIGNAL_INJECTIONS = {
    "inject_early_sigterm": {
        "section": "global",
        "type": "bool",
        "default": False,
        "desc": "Send SIGTERM early in daemon startup (tests clean shutdown)",
    },
}

ALL_INJECTIONS = {
    **MESSENGER_INJECTIONS,
    **MON_INJECTIONS,
    **OBJECTER_INJECTIONS,
    **OSD_INJECTIONS,
    **BLUESTORE_INJECTIONS,
    **FILESTORE_INJECTIONS,
    **RGW_INJECTIONS,
    **MDS_INJECTIONS,
    **CLIENT_INJECTIONS,
    **SIGNAL_INJECTIONS,
}

# =============================================================================
# CHAOS PROFILES — Predefined combinations for common fault scenarios
# =============================================================================

PROFILES = {
    "network_chaos": {
        "desc": "Simulate unreliable network between daemons",
        "configs": {
            "ms_inject_socket_failures": 500,
            "ms_inject_delay_type": "osd",
            "ms_inject_delay_max": 5,
            "ms_inject_delay_probability": 0.05,
        },
    },
    "network_latency": {
        "desc": "Inject message latency without dropping connections",
        "configs": {
            "ms_inject_delay_type": "osd",
            "ms_inject_delay_max": 3,
            "ms_inject_delay_probability": 0.1,
            "ms_inject_internal_delays": 0.5,
        },
    },
    "storage_corruption": {
        "desc": "Inject storage-layer read errors and checksum failures",
        "configs": {
            "bluestore_debug_inject_read_err": True,
            "bluestore_debug_inject_csum_err_probability": 0.01,
        },
    },
    "bdev_crash": {
        "desc": "Simulate block device crashes (SSD firmware failure)",
        "configs": {
            "bdev_inject_crash": 10000,
            "bdev_inject_crash_flush_delay": 5,
        },
    },
    "mon_instability": {
        "desc": "Destabilize MON quorum via paxos and scrub injection",
        "configs": {
            "mon_scrub_inject_crc_mismatch": 0.1,
            "mon_scrub_inject_missing_keys": 0.05,
            "mon_inject_transaction_delay_max": 10,
            "mon_inject_transaction_delay_probability": 0.1,
        },
    },
    "mon_sync_delay": {
        "desc": "Slow down MON synchronization (new MON joining)",
        "configs": {
            "mon_inject_sync_get_chunk_delay": 5.0,
        },
    },
    "mds_chaos": {
        "desc": "Inject MDS journal corruption and migration races",
        "configs": {
            "mds_inject_health_dummy": True,
            "mds_inject_traceless_reply_probability": 0.05,
            "mds_inject_migrator_session_race": True,
        },
    },
    "mds_corruption": {
        "desc": "Inject dentry corruption in MDS rename/journal paths",
        "configs": {
            "mds_inject_rename_corrupt_dentry_first": 0.01,
            "mds_inject_journal_corrupt_dentry_first": 0.01,
        },
    },
    "rgw_sync_errors": {
        "desc": "Inject errors into RGW multi-site sync paths",
        "configs": {
            "rgw_sync_data_inject_err_probability": 0.01,
            "rgw_sync_meta_inject_err_probability": 0.01,
            "rgw_data_sync_single_entry_inject_err_probability": 0.01,
        },
    },
    "rgw_latency": {
        "desc": "Inject delays at RGW injection points and OLH operations",
        "configs": {
            "rgw_inject_delay_sec": 2.0,
            "rgw_inject_delay_pattern": "delay_bucket_full_sync_loop",
            "rgw_debug_inject_latency_bi_unlink": 3,
        },
    },
    "rgw_multipart_errors": {
        "desc": "Inject multipart upload lock errors",
        "configs": {
            "rgw_mp_lock_inject_delay": 5,
            "rgw_mp_lock_inject_renewal_error": -5,
        },
    },
    "osd_dispatch_delays": {
        "desc": "Slow OSD op dispatch to trigger client-side timeouts",
        "configs": {
            "osd_debug_inject_dispatch_delay_probability": 0.05,
            "osd_debug_inject_dispatch_delay_duration": 2,
        },
    },
    "osd_map_corruption": {
        "desc": "Inject bad CRC in OSD maps and PG removal failures",
        "configs": {
            "osd_inject_bad_map_crc_probability": 0.01,
            "osd_inject_failure_on_pg_removal": True,
        },
    },
    "client_faults": {
        "desc": "Inject client-side cap release and session failures",
        "configs": {
            "client_inject_release_failure": True,
            "client_debug_inject_tick_delay": 5,
        },
    },
    "heartbeat_failure": {
        "desc": "Simulate OSD heartbeat failures causing false down reports",
        "configs": {
            "heartbeat_inject_failure": 500,
        },
    },
    "bluestore_full_corruption": {
        "desc": "BlueStore read errors + checksum errors + alloc failures",
        "configs": {
            "bluestore_debug_inject_read_err": True,
            "bluestore_debug_inject_csum_err_probability": 0.01,
            "bluestore_debug_inject_allocation_from_file_failure": 0.01,
        },
    },
    "rgw_olh_errors": {
        "desc": "Inject errors in RGW object versioning and OLH operations",
        "configs": {
            "rgw_debug_inject_set_olh_err": 5,
            "rgw_debug_inject_olh_cancel_modification_err": True,
            "rgw_inject_notify_timeout_probability": 0.05,
        },
    },
    "mon_election_stress": {
        "desc": "Stress MON elections with paxos delays and PG merge bounces",
        "configs": {
            "mon_inject_transaction_delay_probability": 0.15,
            "mon_inject_transaction_delay_max": 5,
            "mon_inject_pg_merge_bounce_probability": 0.1,
        },
    },
    "client_session_stress": {
        "desc": "Stress client sessions with cap failures and stale TIDs",
        "configs": {
            "client_inject_release_failure": True,
            "client_inject_fixed_oldest_tid": True,
            "client_debug_inject_tick_delay": 3,
        },
    },
    "objecter_faults": {
        "desc": "Inject objecter watch timeout and relock delays",
        "configs": {
            "objecter_inject_no_watch_ping": True,
            "objecter_debug_inject_relock_delay": True,
        },
    },
    "rgw_full_sync_chaos": {
        "desc": "Inject errors across all RGW sync paths simultaneously",
        "configs": {
            "rgw_sync_data_inject_err_probability": 0.02,
            "rgw_sync_meta_inject_err_probability": 0.02,
            "rgw_sync_data_full_inject_err_probability": 0.02,
            "rgw_data_sync_single_entry_inject_err_probability": 0.02,
            "rgw_inject_notify_timeout_probability": 0.05,
        },
    },
    "multi_layer_chaos": {
        "desc": "Combined network delays + storage corruption + OSD dispatch",
        "configs": {
            "ms_inject_delay_type": "osd",
            "ms_inject_delay_max": 2,
            "ms_inject_delay_probability": 0.03,
            "bluestore_debug_inject_read_err": True,
            "osd_debug_inject_dispatch_delay_probability": 0.03,
            "osd_debug_inject_dispatch_delay_duration": 1,
        },
    },
}


class CephErrorInjector:
    """
    Unified Ceph error injection framework for thrash and stress testing.

    Provides a structured API to inject faults into a running Ceph cluster
    via MON config database (persistent, cluster-wide) and admin socket
    commands (per-daemon, runtime-only). Tracks all active injections for
    guaranteed cleanup.

    Initialization:
        injector = CephErrorInjector(rados_obj=rados_obj)

    The class follows the same composition pattern as MonConfigMethods --
    it takes a RadosOrchestrator instance and delegates cluster commands
    through it.

    Suite YAML Integration — Two Approaches:

    1. Embedded (inside test config):
        Pass an `error_injection` dict directly in a test's config block:

            - test:
                module: test_osd_thrashing.py
                config:
                  error_injection:
                    profile: network_chaos

    2. Standalone (separate inject/cleanup steps):
        Use test_ceph_error_injection.py as suite-level steps around
        any tests. No code changes needed in the tests themselves:

            - test:
                name: Inject errors
                module: test_ceph_error_injection.py
                abort-on-fail: true
                config:
                  action: inject
                  error_injection:
                    profile: mon_instability

            - test:
                name: Run any test with faults active
                module: some_other_test.py
                config: ...

            - test:
                name: Cleanup errors
                module: test_ceph_error_injection.py
                config:
                  action: cleanup

    Supported error_injection keys:
        - profile (str): Name of a predefined chaos profile
        - profile_overrides (dict): Override values within the profile
        - configs (dict): Key-value pairs of inject config options
        - ec_write_errors (list): EC write error injection specs
        - ec_read_errors (list): EC read error injection specs
        - admin_socket (list): Arbitrary admin socket injection commands
    """

    def __init__(self, rados_obj: RadosOrchestrator):
        """
        Initialize the error injector.

        Args:
            rados_obj: RadosOrchestrator instance for cluster communication
        """
        self.rados_obj = rados_obj
        self.mon_obj = MonConfigMethods(rados_obj=rados_obj)
        self._active_config_injections = []
        self._active_daemon_injections = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup_all()
        return False

    # =========================================================================
    # CONFIG-BASED INJECTION (cluster-wide via MON config DB)
    # =========================================================================

    def inject_config(self, name: str, value, section: str = None) -> bool:
        """
        Set a single inject config variable in the MON config database.

        The variable is validated against the known catalog. Section is
        auto-resolved from the catalog if not provided.

        Args:
            name: Config key name (must be in ALL_INJECTIONS catalog)
            value: Value to set (type validated against catalog)
            section: Override target section (default: from catalog)

        Returns:
            True if successfully set, False otherwise

        Raises:
            ValueError: If name is not in the catalog
        """
        if name not in ALL_INJECTIONS:
            raise ValueError(
                f"Unknown inject variable: '{name}'. "
                f"Use CephErrorInjector.list_available() to see all options."
            )

        meta = ALL_INJECTIONS[name]

        if meta.get("runtime") is False:
            log.warning(
                f"Config '{name}' is NOT runtime-updatable. "
                "It requires daemon restart or client remount to take effect."
            )

        if meta["type"] == "float" and name.endswith("_probability"):
            try:
                fval = float(value)
                if fval < 0.0 or fval > 1.0:
                    log.warning(
                        f"Config '{name}' is a probability but value "
                        f"{fval} is outside 0.0-1.0 range. "
                        f"Did you mean {fval / 100.0}?"
                    )
            except (TypeError, ValueError):
                pass

        # Safety check: some "every Nth operation" configs with very
        # low values (e.g., 1) will break cluster connectivity entirely
        # because every messenger/heartbeat/dispatch operation fails.
        # Configs where N = duration of fault (ANY non-zero is risky)
        _DURATION_DANGER = {
            "ms_inject_network_congestion",
        }
        # Configs where low int/uint values are dangerous (every-Nth)
        _LOW_VALUE_DANGER = {
            "ms_inject_socket_failures": 100,
            "heartbeat_inject_failure": 100,
            "bdev_inject_crash": 1000,
        }
        # Configs where high float/int values are dangerous (durations)
        _HIGH_VALUE_DANGER = {
            "osd_debug_inject_dispatch_delay_duration": 30,
            "ms_inject_delay_max": 30,
            "mon_inject_transaction_delay_max": 60,
            "filestore_inject_stall": 60,
            "client_debug_inject_tick_delay": 60,
            "rgw_mp_lock_inject_delay": 60,
        }
        try:
            num_val = float(value)
            if name in _DURATION_DANGER and num_val > 0:
                log.warning(
                    f"SAFETY: Config '{name}' set to {num_val}. "
                    f"N = how long the fault lasts (in operations), "
                    f"NOT frequency. Any non-zero value is risky."
                )
            elif name in _LOW_VALUE_DANGER:
                min_safe = _LOW_VALUE_DANGER[name]
                if 0 < num_val < min_safe:
                    log.warning(
                        f"SAFETY: Config '{name}' set to {num_val} is "
                        f"dangerously aggressive (min safe ~{min_safe})."
                    )
            elif name in _HIGH_VALUE_DANGER:
                max_safe = _HIGH_VALUE_DANGER[name]
                if num_val > max_safe:
                    log.warning(
                        f"SAFETY: Config '{name}' set to {num_val}s "
                        f"exceeds safe threshold ({max_safe}s). "
                        f"This may cause command timeouts."
                    )
        except (TypeError, ValueError):
            pass

        target_section = section or meta["section"]
        str_value = self._convert_value(value, meta["type"])

        log.info(
            f"Injecting config: ceph config set {target_section} " f"{name} {str_value}"
        )

        # Run ceph config set directly instead of going through MonConfigMethods.set_config()
        cmd = f"ceph config set {target_section} {name} {str_value}"
        try:
            self.rados_obj.node.shell([cmd])
        except Exception as e:
            log.error(f"Failed to run ceph config set for {name}: {e}")
            return False

        # Verify the config was actually applied using float-aware
        # comparison against ceph config dump
        if not self._verify_config_set(target_section, name, str_value):
            log.warning(
                f"Config {name} may not have been set correctly, "
                f"but proceeding with tracking for cleanup"
            )

        self._active_config_injections.append({"name": name, "section": target_section})
        log.info(f"Injection active: {name} = {str_value} [{target_section}]")
        return True

    def inject_config_batch(self, injections: dict) -> dict:
        """
        Apply multiple config injections at once.

        Args:
            injections: Dict of {config_key: value} pairs.
                        Keys must be in ALL_INJECTIONS catalog.

        Returns:
            Dict of {config_key: bool} indicating success/failure per key

        Example:
            results = injector.inject_config_batch({
                "ms_inject_delay_probability": 0.1,
                "ms_inject_delay_max": 5,
                "osd_debug_inject_dispatch_delay_probability": 0.2,
            })
        """
        results = {}
        for name, value in injections.items():
            try:
                results[name] = self.inject_config(name, value)
            except ValueError as e:
                log.error(str(e))
                results[name] = False
        return results

    def remove_config_injection(self, name: str, section: str = None) -> bool:
        """
        Remove a single inject config variable from the MON config database.

        Args:
            name: Config key name
            section: Override target section (default: from catalog)

        Returns:
            True if successfully removed, False otherwise
        """
        if name not in ALL_INJECTIONS:
            log.warning(f"Unknown inject variable: '{name}', attempting removal anyway")
            target_section = section or "global"
        else:
            target_section = section or ALL_INJECTIONS[name]["section"]

        log.info(f"Removing injection: {name} from [{target_section}]")
        result = self.mon_obj.remove_config(
            section=target_section, name=name, verify_rm=False
        )

        self._active_config_injections = [
            inj
            for inj in self._active_config_injections
            if not (inj["name"] == name and inj["section"] == target_section)
        ]

        return result

    # =========================================================================
    # ADMIN SOCKET INJECTION (per-daemon, requires host resolution)
    # =========================================================================

    def inject_ec_write_error(
        self,
        osd_id: int,
        pool: str,
        obj: str = "*",
        shard: int = 2,
        err_type: int = 1,
        skip: int = 0,
        duration: int = 1000,
    ) -> bool:
        """
        Inject write errors for objects in an EC pool via OSD admin socket.

        Runs: ceph daemon osd.{id} injectecwriteerr <pool> <obj> <shard>
              <type> <skip> <duration>

        Args:
            osd_id: Target OSD ID
            pool: EC pool name
            obj: Object name or '*' for all objects
            shard: Shard ID to inject on (default 2)
            err_type: Error type (1=drop sub write, default 1)
            skip: Number of writes to skip before injecting (default 0)
            duration: Number of writes to inject errors on (default 1000)

        Returns:
            True if injection command succeeded, False otherwise
        """
        cmd = (
            f"cephadm shell -- ceph daemon osd.{osd_id} injectecwriteerr "
            f"{pool} '{obj}' {shard} {err_type} {skip} {duration}"
        )
        result = self._run_on_osd_host(osd_id, cmd)

        if result:
            self._active_daemon_injections.append(
                {
                    "type": "ec_write",
                    "osd_id": osd_id,
                    "pool": pool,
                    "obj": obj,
                    "shard": shard,
                    "err_type": err_type,
                }
            )

        return result

    def inject_ec_read_error(
        self,
        osd_id: int,
        pool: str,
        obj: str = "*",
        shard: int = 2,
        err_type: int = 1,
        skip: int = 0,
        duration: int = 1000,
    ) -> bool:
        """
        Inject read errors for objects in an EC pool via OSD admin socket.

        Runs: ceph daemon osd.{id} injectecreaderr <pool> <obj> <shard>
              <type> <skip> <duration>

        Args:
            osd_id: Target OSD ID
            pool: EC pool name
            obj: Object name or '*' for all objects
            shard: Shard ID to inject on (default 2)
            err_type: Error type (default 1)
            skip: Number of reads to skip before injecting (default 0)
            duration: Number of reads to inject errors on (default 1000)

        Returns:
            True if injection command succeeded, False otherwise
        """
        cmd = (
            f"cephadm shell -- ceph daemon osd.{osd_id} injectecreaderr "
            f"{pool} '{obj}' {shard} {err_type} {skip} {duration}"
        )
        result = self._run_on_osd_host(osd_id, cmd)

        if result:
            self._active_daemon_injections.append(
                {
                    "type": "ec_read",
                    "osd_id": osd_id,
                    "pool": pool,
                    "obj": obj,
                    "shard": shard,
                    "err_type": err_type,
                }
            )

        return result

    def clear_ec_write_error(
        self,
        osd_id: int,
        pool: str,
        obj: str = "*",
        shard: int = 2,
        err_type: int = 1,
    ) -> bool:
        """
        Clear previously injected EC write errors.

        Runs: ceph daemon osd.{id} injectecclearwriteerr <pool> <obj>
              <shard> <type>

        Args:
            osd_id: Target OSD ID
            pool: EC pool name
            obj: Object name or '*'
            shard: Shard ID
            err_type: Error type to clear

        Returns:
            True if clear command succeeded, False otherwise
        """
        cmd = (
            f"cephadm shell -- ceph daemon osd.{osd_id} injectecclearwriteerr "
            f"{pool} '{obj}' {shard} {err_type}"
        )
        result = self._run_on_osd_host(osd_id, cmd)

        if result:
            self._active_daemon_injections = [
                inj
                for inj in self._active_daemon_injections
                if not (
                    inj.get("type") == "ec_write"
                    and inj.get("osd_id") == osd_id
                    and inj.get("pool") == pool
                    and inj.get("shard") == shard
                )
            ]

        return result

    def clear_ec_read_error(
        self,
        osd_id: int,
        pool: str,
        obj: str = "*",
        shard: int = 2,
        err_type: int = 1,
    ) -> bool:
        """
        Clear previously injected EC read errors.

        Runs: ceph daemon osd.{id} injectecclearreaderr <pool> <obj>
              <shard> <type>

        Args:
            osd_id: Target OSD ID
            pool: EC pool name
            obj: Object name or '*'
            shard: Shard ID
            err_type: Error type to clear

        Returns:
            True if clear command succeeded, False otherwise
        """
        cmd = (
            f"cephadm shell -- ceph daemon osd.{osd_id} injectecclearreaderr "
            f"{pool} '{obj}' {shard} {err_type}"
        )
        result = self._run_on_osd_host(osd_id, cmd)

        if result:
            self._active_daemon_injections = [
                inj
                for inj in self._active_daemon_injections
                if not (
                    inj.get("type") == "ec_read"
                    and inj.get("osd_id") == osd_id
                    and inj.get("pool") == pool
                    and inj.get("shard") == shard
                )
            ]

        return result

    def inject_data_error(
        self, osd_id: int, pool: str, objname: str, shard: int = None
    ) -> bool:
        """
        Inject data corruption on a specific object.

        Runs: ceph daemon osd.{id} injectdataerr <pool> <objname> [shard]

        Args:
            osd_id: Target OSD ID
            pool: Pool name
            objname: Object name
            shard: Optional shard ID for EC pools

        Returns:
            True if injection succeeded, False otherwise
        """
        cmd = (
            f"cephadm shell -- ceph daemon osd.{osd_id} injectdataerr "
            f"{pool} {objname}"
        )
        if shard is not None:
            cmd += f" {shard}"

        result = self._run_on_osd_host(osd_id, cmd)

        if result:
            self._active_daemon_injections.append(
                {
                    "type": "data_err",
                    "osd_id": osd_id,
                    "pool": pool,
                    "obj": objname,
                    "shard": shard,
                }
            )

        return result

    def inject_metadata_error(
        self, osd_id: int, pool: str, objname: str, shard: int = None
    ) -> bool:
        """
        Inject metadata corruption on a specific object.

        Runs: ceph daemon osd.{id} injectmdataerr <pool> <objname> [shard]

        Args:
            osd_id: Target OSD ID
            pool: Pool name
            objname: Object name
            shard: Optional shard ID for EC pools

        Returns:
            True if injection succeeded, False otherwise
        """
        cmd = (
            f"cephadm shell -- ceph daemon osd.{osd_id} injectmdataerr "
            f"{pool} {objname}"
        )
        if shard is not None:
            cmd += f" {shard}"

        result = self._run_on_osd_host(osd_id, cmd)

        if result:
            self._active_daemon_injections.append(
                {
                    "type": "mdata_err",
                    "osd_id": osd_id,
                    "pool": pool,
                    "obj": objname,
                    "shard": shard,
                }
            )

        return result

    def inject_full(self, osd_id: int, full_type: str = "full", count: int = 1) -> bool:
        """
        Simulate disk full condition on an OSD.

        Runs: ceph daemon osd.{id} injectfull [type] [count]

        Args:
            osd_id: Target OSD ID
            full_type: Type of full condition
                       ("full", "nearfull", "backfillfull")
            count: Number of times to inject (default 1)

        Returns:
            True if injection succeeded, False otherwise
        """
        cmd = (
            f"cephadm shell -- ceph daemon osd.{osd_id} injectfull "
            f"{full_type} {count}"
        )
        result = self._run_on_osd_host(osd_id, cmd)

        if result:
            self._active_daemon_injections.append(
                {
                    "type": "full",
                    "osd_id": osd_id,
                    "full_type": full_type,
                }
            )

        return result

    def inject_bluefs_read_zeros(self, osd_id: int) -> bool:
        """
        Inject 8K of zeros into the next BlueFS read.

        Runs: ceph daemon osd.{id} bluefs debug_inject_read_zeros

        Args:
            osd_id: Target OSD ID

        Returns:
            True if injection succeeded, False otherwise
        """
        cmd = (
            f"cephadm shell -- ceph daemon osd.{osd_id} "
            f"bluefs debug_inject_read_zeros"
        )
        result = self._run_on_osd_host(osd_id, cmd)

        if result:
            self._active_daemon_injections.append(
                {"type": "bluefs_zeros", "osd_id": osd_id}
            )

        return result

    def inject_args(self, daemon_type: str, daemon_id: str, args: dict) -> bool:
        """
        Inject configuration arguments into a running daemon via ceph tell.

        This bypasses the MON config database entirely -- the args are
        applied directly to the daemon's runtime config and do NOT persist
        across restarts.

        Runs: ceph tell {daemon_type}.{daemon_id} injectargs --key value ...

        Args:
            daemon_type: Daemon type (osd, mon, mds, mgr)
            daemon_id: Daemon ID (e.g., "0", "*" for all)
            args: Dict of {config_key: value} to inject

        Returns:
            True if injection succeeded, False otherwise
        """
        args_str = " ".join(f"--{k} {v}" for k, v in args.items())
        cmd = f"ceph tell {daemon_type}.{daemon_id} injectargs {args_str}"

        log.info(f"Injecting args: {cmd}")
        try:
            self.rados_obj.node.shell([cmd])
            log.info(f"Successfully injected args on {daemon_type}.{daemon_id}")
            self._active_daemon_injections.append(
                {
                    "type": "injectargs",
                    "daemon_type": daemon_type,
                    "daemon_id": daemon_id,
                    "args": args,
                }
            )
            return True
        except Exception as e:
            log.error(f"Failed to inject args on {daemon_type}.{daemon_id}: {e}")
            return False

    # =========================================================================
    # CHAOS PROFILES
    # =========================================================================

    def apply_profile(self, profile_name: str, **overrides) -> dict:
        """
        Apply a predefined chaos profile (batch of related injections).

        Profiles are predefined combinations of inject variables that
        simulate common failure scenarios. Override individual values
        within a profile by passing them as keyword arguments.

        Args:
            profile_name: Name of the profile (see PROFILES dict)
            **overrides: Override specific values within the profile

        Returns:
            Dict of {config_key: bool} indicating success/failure per key

        Raises:
            ValueError: If profile_name is not found

        Example:
            injector.apply_profile("network_chaos")
            injector.apply_profile("storage_corruption",
                bluestore_debug_inject_csum_err_probability=0.05)

        Available profiles:
            - network_chaos: Unreliable network (socket failures + delays)
            - network_latency: Message latency without drops
            - storage_corruption: BlueStore read/checksum errors
            - bdev_crash: Block device crash simulation
            - mon_instability: MON scrub + paxos delays
            - mon_sync_delay: Slow MON synchronization
            - mds_chaos: MDS health + migration races
            - mds_corruption: MDS dentry corruption
            - rgw_sync_errors: RGW multi-site sync failures
            - rgw_latency: RGW delay injection points
            - rgw_multipart_errors: Multipart upload lock errors
            - osd_dispatch_delays: OSD op dispatch delays
            - osd_map_corruption: OSD map CRC + PG removal failures
            - client_faults: Client cap release + tick delays
            - heartbeat_failure: OSD heartbeat injection
            - bluestore_full_corruption: Read + checksum + alloc errors
            - rgw_olh_errors: OLH versioning errors + notify timeout
            - mon_election_stress: Paxos delays + PG merge bounces
            - client_session_stress: Cap failures + stale TIDs
            - objecter_faults: Watch timeout + relock delays
            - rgw_full_sync_chaos: All RGW sync error paths
            - multi_layer_chaos: Network + storage + dispatch combined
        """
        if profile_name not in PROFILES:
            available = ", ".join(sorted(PROFILES.keys()))
            raise ValueError(
                f"Unknown profile: '{profile_name}'. " f"Available: {available}"
            )

        profile = PROFILES[profile_name]
        configs = dict(profile["configs"])
        configs.update(overrides)

        log.info(
            f"Applying chaos profile '{profile_name}': {profile['desc']} "
            f"({len(configs)} configs)"
        )

        return self.inject_config_batch(configs)

    # =========================================================================
    # SUITE YAML CONFIGURATION
    # =========================================================================

    def validate_config(self, error_injection_config: dict) -> dict:
        """
        Dry-run validation of a suite YAML error_injection config dict.

        Checks for typos, invalid keys, unknown profiles, and malformed
        specs WITHOUT actually applying anything. Call this before
        apply_from_config() to catch configuration errors early.

        Args:
            error_injection_config: Dict with the same schema as
                                    apply_from_config() accepts.

        Returns:
            Dict with validation results:
            {
                "valid": bool,
                "errors": list of error strings,
                "warnings": list of warning strings,
            }

        Example:
            result = injector.validate_config(config.get("error_injection"))
            if not result["valid"]:
                for err in result["errors"]:
                    log.error(f"Config error: {err}")
                return 1
        """
        errors = []
        warnings = []

        if not error_injection_config:
            return {"valid": True, "errors": [], "warnings": []}

        valid_top_keys = {
            "profile",
            "profile_overrides",
            "configs",
            "ec_write_errors",
            "ec_read_errors",
            "admin_socket",
        }
        for key in error_injection_config:
            if key not in valid_top_keys:
                errors.append(
                    f"Unknown top-level key '{key}'. "
                    f"Valid keys: {sorted(valid_top_keys)}"
                )

        # Validate profile
        profile_name = error_injection_config.get("profile")
        if profile_name and profile_name not in PROFILES:
            available = ", ".join(sorted(PROFILES.keys()))
            errors.append(f"Unknown profile '{profile_name}'. Available: {available}")

        # Validate profile_overrides keys
        for key in error_injection_config.get("profile_overrides", {}):
            if key not in ALL_INJECTIONS:
                errors.append(f"Unknown inject variable in profile_overrides: '{key}'")

        # Validate individual configs
        for key in error_injection_config.get("configs", {}):
            if key not in ALL_INJECTIONS:
                errors.append(f"Unknown inject variable in configs: '{key}'")
            elif ALL_INJECTIONS[key].get("runtime") is False:
                warnings.append(
                    f"Config '{key}' is NOT runtime-updatable. "
                    "Requires daemon restart or client remount."
                )

        # Validate EC write error specs
        for i, spec in enumerate(error_injection_config.get("ec_write_errors", [])):
            if not isinstance(spec, dict):
                errors.append(f"ec_write_errors[{i}]: must be a dict")
                continue
            if "osd_id" not in spec:
                errors.append(f"ec_write_errors[{i}]: missing 'osd_id'")
            if "pool" not in spec:
                errors.append(f"ec_write_errors[{i}]: missing 'pool'")

        # Validate EC read error specs
        for i, spec in enumerate(error_injection_config.get("ec_read_errors", [])):
            if not isinstance(spec, dict):
                errors.append(f"ec_read_errors[{i}]: must be a dict")
                continue
            if "osd_id" not in spec:
                errors.append(f"ec_read_errors[{i}]: missing 'osd_id'")
            if "pool" not in spec:
                errors.append(f"ec_read_errors[{i}]: missing 'pool'")

        # Validate admin socket specs
        valid_asok_cmds = {
            "injectfull",
            "injectdataerr",
            "injectmdataerr",
            "bluefs_read_zeros",
            "injectargs",
        }
        for i, spec in enumerate(error_injection_config.get("admin_socket", [])):
            if not isinstance(spec, dict):
                errors.append(f"admin_socket[{i}]: must be a dict")
                continue
            command = spec.get("command")
            if not command:
                errors.append(f"admin_socket[{i}]: missing 'command'")
            elif command not in valid_asok_cmds:
                errors.append(
                    f"admin_socket[{i}]: unknown command '{command}'. "
                    f"Valid: {sorted(valid_asok_cmds)}"
                )
            if command in ("injectfull", "bluefs_read_zeros"):
                if "osd_id" not in spec:
                    errors.append(f"admin_socket[{i}]: '{command}' requires 'osd_id'")
            elif command in ("injectdataerr", "injectmdataerr"):
                for req in ("osd_id", "pool", "objname"):
                    if req not in spec:
                        errors.append(
                            f"admin_socket[{i}]: '{command}' " f"requires '{req}'"
                        )
            elif command == "injectargs":
                for req in ("daemon_type", "daemon_id", "args"):
                    if req not in spec:
                        errors.append(
                            f"admin_socket[{i}]: '{command}' " f"requires '{req}'"
                        )

        is_valid = len(errors) == 0

        if errors:
            log.error(
                f"Error injection config validation failed with "
                f"{len(errors)} error(s):"
            )
            for err in errors:
                log.error(f"  - {err}")
        if warnings:
            for warn in warnings:
                log.warning(f"  - {warn}")
        if is_valid:
            log.info("Error injection config validation passed")

        return {"valid": is_valid, "errors": errors, "warnings": warnings}

    def apply_from_config(self, error_injection_config: dict) -> dict:
        """
        Apply error injections from a suite YAML config dict.

        This method is the primary integration point for suite-driven
        injection. It reads a structured dict (typically from
        config["error_injection"]) and applies all specified injections.

        Args:
            error_injection_config: Dict with optional keys:
                - profile (str): Chaos profile name to apply
                - profile_overrides (dict): Overrides for the profile
                - configs (dict): Additional individual config injections
                - ec_write_errors (list): EC write error specs
                - ec_read_errors (list): EC read error specs
                - admin_socket (list): Admin socket command specs

        Returns:
            Dict summarizing what was applied:
            {
                "profile": str or None,
                "configs_applied": int,
                "configs_failed": int,
                "ec_errors_injected": int,
                "admin_commands_run": int,
            }

        Suite YAML example:
            config:
              error_injection:
                profile: network_chaos
                profile_overrides:
                  ms_inject_delay_probability: 0.2
                configs:
                  osd_debug_inject_dispatch_delay_probability: 0.1
                  mds_inject_health_dummy: true
                ec_write_errors:
                  - osd_id: 0
                    pool: my-ec-pool
                    shard: 2
                    duration: 500
                admin_socket:
                  - command: injectfull
                    osd_id: 2
                    full_type: nearfull
                    count: 1
        """
        if not error_injection_config:
            log.debug("No error_injection config provided, skipping")
            return {
                "profile": None,
                "configs_applied": 0,
                "configs_failed": 0,
                "ec_errors_injected": 0,
                "admin_commands_run": 0,
            }

        # Validate before applying to catch typos early
        validation = self.validate_config(error_injection_config)
        if not validation["valid"]:
            log.error(
                "Error injection config has validation errors. "
                "Proceeding with best-effort application."
            )

        summary = {
            "profile": None,
            "configs_applied": 0,
            "configs_failed": 0,
            "ec_errors_injected": 0,
            "admin_commands_run": 0,
        }

        log.info(
            f"Applying error injection from config: "
            f"{json.dumps(error_injection_config, indent=2)}"
        )

        # 1. Apply profile if specified
        profile_name = error_injection_config.get("profile")
        if profile_name:
            overrides = error_injection_config.get("profile_overrides", {})
            results = self.apply_profile(profile_name, **overrides)
            summary["profile"] = profile_name
            summary["configs_applied"] += sum(1 for v in results.values() if v)
            summary["configs_failed"] += sum(1 for v in results.values() if not v)

        # 2. Apply individual config injections
        configs = error_injection_config.get("configs", {})
        if configs:
            results = self.inject_config_batch(configs)
            summary["configs_applied"] += sum(1 for v in results.values() if v)
            summary["configs_failed"] += sum(1 for v in results.values() if not v)

        # 3. Auto-enable prerequisites for EC and data/metadata error injection
        has_ec_errors = bool(
            error_injection_config.get("ec_write_errors")
            or error_injection_config.get("ec_read_errors")
        )
        has_data_errors = any(
            spec.get("command") in ("injectdataerr", "injectmdataerr")
            for spec in error_injection_config.get("admin_socket", [])
        )

        if has_ec_errors or has_data_errors:
            already_set = any(
                inj["name"] == "bluestore_debug_inject_read_err"
                for inj in self._active_config_injections
            )
            if not already_set:
                log.info(
                    "Auto-enabling prerequisite: "
                    "bluestore_debug_inject_read_err=true "
                    "(required for EC/data error injection)"
                )
                prereq_ok = self.inject_config("bluestore_debug_inject_read_err", True)
                if prereq_ok:
                    summary["configs_applied"] += 1
                else:
                    summary["configs_failed"] += 1

        # 5. Apply EC write error injections
        for spec in error_injection_config.get("ec_write_errors", []):
            success = self.inject_ec_write_error(
                osd_id=spec["osd_id"],
                pool=spec["pool"],
                obj=spec.get("obj", "*"),
                shard=spec.get("shard", 2),
                err_type=spec.get("err_type", 1),
                skip=spec.get("skip", 0),
                duration=spec.get("duration", 1000),
            )
            if success:
                summary["ec_errors_injected"] += 1

        # 6. Apply EC read error injections
        for spec in error_injection_config.get("ec_read_errors", []):
            success = self.inject_ec_read_error(
                osd_id=spec["osd_id"],
                pool=spec["pool"],
                obj=spec.get("obj", "*"),
                shard=spec.get("shard", 2),
                err_type=spec.get("err_type", 1),
                skip=spec.get("skip", 0),
                duration=spec.get("duration", 1000),
            )
            if success:
                summary["ec_errors_injected"] += 1

        # 7. Apply admin socket commands
        for spec in error_injection_config.get("admin_socket", []):
            command = spec.get("command")
            success = False
            if command == "injectfull":
                success = self.inject_full(
                    osd_id=spec["osd_id"],
                    full_type=spec.get("full_type", "full"),
                    count=spec.get("count", 1),
                )
            elif command == "injectdataerr":
                success = self.inject_data_error(
                    osd_id=spec["osd_id"],
                    pool=spec["pool"],
                    objname=spec["objname"],
                    shard=spec.get("shard"),
                )
            elif command == "injectmdataerr":
                success = self.inject_metadata_error(
                    osd_id=spec["osd_id"],
                    pool=spec["pool"],
                    objname=spec["objname"],
                    shard=spec.get("shard"),
                )
            elif command == "bluefs_read_zeros":
                success = self.inject_bluefs_read_zeros(osd_id=spec["osd_id"])
            elif command == "injectargs":
                success = self.inject_args(
                    daemon_type=spec["daemon_type"],
                    daemon_id=spec["daemon_id"],
                    args=spec["args"],
                )
            else:
                log.warning(f"Unknown admin_socket command: {command}")

            if success:
                summary["admin_commands_run"] += 1

        log.info(
            f"Error injection applied: profile={summary['profile']}, "
            f"configs={summary['configs_applied']} ok / "
            f"{summary['configs_failed']} failed, "
            f"ec_errors={summary['ec_errors_injected']}, "
            f"admin_cmds={summary['admin_commands_run']}"
        )

        return summary

    # =========================================================================
    # CLEANUP
    # =========================================================================

    def cleanup_all(self) -> dict:
        """
        Remove ALL active injections (config and admin socket).

        This method is idempotent and safe to call multiple times. It
        attempts to clean up every tracked injection, logging failures
        but not raising exceptions.

        Returns:
            Dict with cleanup summary:
            {
                "configs_removed": int,
                "configs_failed": int,
                "daemon_cmds_cleared": int,
                "daemon_cmds_failed": int,
            }
        """
        summary = {
            "configs_removed": 0,
            "configs_failed": 0,
            "daemon_cmds_cleared": 0,
            "daemon_cmds_failed": 0,
        }

        if not self._active_config_injections and not self._active_daemon_injections:
            log.debug("No active injections to clean up")
            return summary

        log.info(
            f"Cleaning up {len(self._active_config_injections)} config "
            f"injections and {len(self._active_daemon_injections)} daemon "
            f"injections"
        )

        # Clean config injections
        for inj in list(self._active_config_injections):
            try:
                self.mon_obj.remove_config(
                    section=inj["section"], name=inj["name"], verify_rm=False
                )
                summary["configs_removed"] += 1
                log.debug(f"Removed config: {inj['name']} [{inj['section']}]")
            except Exception as e:
                log.warning(f"Failed to remove config {inj['name']}: {e}")
                summary["configs_failed"] += 1

        self._active_config_injections.clear()

        # Clean daemon injections
        for inj in list(self._active_daemon_injections):
            try:
                cleared = self._cleanup_daemon_injection(inj)
                if cleared:
                    summary["daemon_cmds_cleared"] += 1
                else:
                    summary["daemon_cmds_failed"] += 1
            except Exception as e:
                log.warning(f"Failed to clear daemon injection {inj}: {e}")
                summary["daemon_cmds_failed"] += 1

        self._active_daemon_injections.clear()

        log.info(
            f"Cleanup complete: configs {summary['configs_removed']} removed / "
            f"{summary['configs_failed']} failed, "
            f"daemon {summary['daemon_cmds_cleared']} cleared / "
            f"{summary['daemon_cmds_failed']} failed"
        )

        return summary

    def get_active_injections(self) -> dict:
        """
        Get current state of all active injections for diagnostics..

        Returns:
            Dict with:
            {
                "config_injections": list of active config injections,
                "daemon_injections": list of active daemon injections,
                "total_active": int,
            }
        """
        return {
            "config_injections": list(self._active_config_injections),
            "daemon_injections": list(self._active_daemon_injections),
            "total_active": (
                len(self._active_config_injections)
                + len(self._active_daemon_injections)
            ),
        }

    # =========================================================================
    # DISCOVERY AND INTROSPECTION
    # =========================================================================

    @staticmethod
    def list_available(component: str = None) -> dict:
        """
        List all available inject variables, optionally filtered by component.

        Args:
            component: Filter by component name. Options:
                       messenger, mon, objecter, osd, bluestore,
                       filestore, rgw, mds, client, signal
                       If None, returns all.

        Returns:
            Dict of {config_key: metadata_dict}
        """
        component_map = {
            "messenger": MESSENGER_INJECTIONS,
            "mon": MON_INJECTIONS,
            "objecter": OBJECTER_INJECTIONS,
            "osd": OSD_INJECTIONS,
            "bluestore": BLUESTORE_INJECTIONS,
            "filestore": FILESTORE_INJECTIONS,
            "rgw": RGW_INJECTIONS,
            "mds": MDS_INJECTIONS,
            "client": CLIENT_INJECTIONS,
            "signal": SIGNAL_INJECTIONS,
        }

        if component:
            component_lower = component.lower()
            if component_lower not in component_map:
                available = ", ".join(sorted(component_map.keys()))
                raise ValueError(
                    f"Unknown component: '{component}'. " f"Available: {available}"
                )
            return component_map[component_lower]

        return ALL_INJECTIONS

    @staticmethod
    def list_profiles() -> dict:
        """
        List all available chaos profiles with descriptions.

        Returns:
            Dict of {profile_name: {"desc": str, "configs": dict}}
        """
        return PROFILES

    @staticmethod
    def get_profile_info(profile_name: str) -> dict:
        """
        Get detailed info about a specific chaos profile.

        Args:
            profile_name: Name of the profile

        Returns:
            Dict with profile description and config keys with their
            metadata from the catalog

        Raises:
            ValueError: If profile_name is not found
        """
        if profile_name not in PROFILES:
            available = ", ".join(sorted(PROFILES.keys()))
            raise ValueError(
                f"Unknown profile: '{profile_name}'. Available: {available}"
            )

        profile = PROFILES[profile_name]
        details = {"desc": profile["desc"], "configs": {}}

        for key, value in profile["configs"].items():
            meta = ALL_INJECTIONS.get(key, {})
            details["configs"][key] = {
                "value_in_profile": value,
                "section": meta.get("section", "unknown"),
                "type": meta.get("type", "unknown"),
                "default": meta.get("default"),
                "desc": meta.get("desc", ""),
            }

        return details

    # =========================================================================
    # INTERNAL HELPERS
    # =========================================================================

    def _verify_config_set(self, section: str, name: str, expected_value: str) -> bool:
        """
        Verify a config was applied using float-aware comparison.

        Ceph stores float values with full precision (e.g., "10.000000"
        for "10.0"), which causes naive string comparison to fail.
        This method handles that by comparing numeric values when the
        config type is float.

        Args:
            section: Config section (mon, osd, global, etc.)
            name: Config key name
            expected_value: The value we set (as string)

        Returns:
            True if the config is found with the expected value
        """
        try:
            cmd = "ceph config dump -f json"
            out, _ = self.rados_obj.node.shell([cmd])
            config_dump = json.loads(out)

            for entry in config_dump:
                if entry.get("name") == name and entry.get("section") == section:
                    actual = entry.get("value", "")
                    if actual == expected_value:
                        return True
                    try:
                        if float(actual) == float(expected_value):
                            return True
                    except (ValueError, TypeError):
                        pass
                    log.warning(
                        f"Config {name} value mismatch: "
                        f"expected={expected_value}, actual={actual}"
                    )
                    return False

            log.warning(
                f"Config {name} not found in ceph config dump "
                f"for section [{section}]"
            )
            return False
        except Exception as e:
            log.warning(f"Failed to verify config {name}: {e}")
            return False

    def _run_on_osd_host(self, osd_id: int, cmd: str) -> bool:
        """
        Resolve the host running an OSD and execute a command on it.

        Args:
            osd_id: OSD ID to resolve
            cmd: Command to execute on the OSD host

        Returns:
            True if command succeeded, False otherwise
        """
        try:
            osd_host = self.rados_obj.fetch_host_node(
                daemon_type="osd", daemon_id=str(osd_id)
            )
            if not osd_host:
                log.error(f"Could not resolve host for osd.{osd_id}")
                return False

            out, err = osd_host.exec_command(cmd=cmd, sudo=True, check_ec=False)
            out_str = str(out).strip() if out else ""
            err_str = str(err).strip() if err else ""

            # Detect actual failures: admin_socket errors, invalid commands,
            # or explicit failure messages. Avoid matching "error" in
            # legitimate output like "clear read error injects".
            failure_patterns = [
                "admin_socket: invalid command",
                "missing required parameter",
                "invalid command",
                "errno",
                "failed to connect",
                "no such file or directory",
            ]
            combined = (out_str + " " + err_str).lower()
            for pattern in failure_patterns:
                if pattern in combined:
                    log.warning(
                        f"Admin socket command failed on osd.{osd_id}: "
                        f"matched '{pattern}' in output: "
                        f"{out_str or err_str}"
                    )
                    return False

            log.info(f"Admin socket command on osd.{osd_id}: {out_str or 'ok'}")
            return True

        except Exception as e:
            log.error(f"Failed to run command on osd.{osd_id} host: {e}")
            return False

    def _cleanup_daemon_injection(self, inj: dict) -> bool:
        """
        Clear a single daemon injection based on its tracked metadata.

        Args:
            inj: Injection tracking dict with 'type' and parameters

        Returns:
            True if cleared, False otherwise
        """
        inj_type = inj.get("type")

        if inj_type == "ec_write":
            cmd = (
                f"cephadm shell -- ceph daemon osd.{inj['osd_id']} "
                f"injectecclearwriteerr {inj['pool']} '{inj['obj']}' "
                f"{inj['shard']} {inj['err_type']}"
            )
            return self._run_on_osd_host(inj["osd_id"], cmd)

        elif inj_type == "ec_read":
            cmd = (
                f"cephadm shell -- ceph daemon osd.{inj['osd_id']} "
                f"injectecclearreaderr {inj['pool']} '{inj['obj']}' "
                f"{inj['shard']} {inj['err_type']}"
            )
            return self._run_on_osd_host(inj["osd_id"], cmd)

        elif inj_type in ("data_err", "mdata_err"):
            log.warning(
                f"Injection '{inj_type}' on osd.{inj.get('osd_id')} "
                f"pool={inj.get('pool')} obj={inj.get('obj')} has no "
                f"clear command. Object corruption persists until "
                f"scrub + repair."
            )
            return True

        elif inj_type in ("full", "bluefs_zeros"):
            log.info(
                f"Injection '{inj_type}' on osd.{inj.get('osd_id')} "
                f"is one-shot and does not require explicit cleanup."
            )
            return True

        elif inj_type == "injectargs":
            log.warning(
                f"Injected args on {inj['daemon_type']}.{inj['daemon_id']} "
                f"bypass MON config DB and persist until daemon restart. "
                f"Args: {inj.get('args', {})}"
            )
            return True

        log.warning(f"Unknown injection type for cleanup: {inj_type}")
        return False

    @staticmethod
    def _convert_value(value, value_type: str) -> str:
        """
        Convert a Python value to the string representation Ceph expects.

        Args:
            value: Python value (bool, int, float, str)
            value_type: Expected Ceph type from catalog

        Returns:
            String representation suitable for `ceph config set`
        """
        if value_type == "bool":
            return "true" if value else "false"
        elif value_type in ("float",):
            return str(float(value))
        elif value_type in ("int", "uint", "secs"):
            return str(int(value))
        else:
            return str(value)


class CephInjectionRecovery:
    """
    Post-injection cluster recovery for persistent damage.

    After error injection configs are removed by CephErrorInjector.cleanup_all(),
    some injections leave persistent damage (corrupted RADOS objects, inconsistent
    PGs, damaged MDS metadata). This class provides recovery workflows to restore
    the cluster to HEALTH_OK.

    Must be called AFTER injector.cleanup_all() -- recovery will not work
    while inject configs are still active.

    Usage:
        injector.cleanup_all()  # Remove all inject configs first
        recovery = CephInjectionRecovery(rados_obj, client_node)
        result = recovery.recover_all(fs_name="cephfs-thrash")
    """

    def __init__(self, rados_obj, client_node=None):
        self.rados_obj = rados_obj
        self.client_node = client_node

    def recover_bluestore_damage(self, timeout=600):
        """
        Recover from BlueStore injection damage (inconsistent PGs, unfound
        objects, spurious read errors).

        Steps:
            1. Identify and repair inconsistent PGs
            2. Handle unfound objects (mark lost if unrecoverable)
            3. Deep-scrub all OSDs
            4. Wait for active+clean PGs
            5. Restart OSDs to clear spurious read error and repair counters

        Returns:
            dict with recovery summary
        """
        result = {
            "pgs_repaired": 0,
            "objects_lost": 0,
            "deep_scrub_run": False,
            "health_ok": False,
            "summary": "",
        }

        log.info("=== BLUESTORE DAMAGE RECOVERY ===")

        # Step 1: Find and repair inconsistent PGs
        log.info("Step 1: Repairing inconsistent PGs")
        try:
            health_out, _ = self.rados_obj.node.shell(["ceph health detail -f json"])
            health = json.loads(health_out)
            checks = health.get("checks", {})

            # Collect PG IDs from health detail
            damaged_pgs = set()
            for check_name in [
                "PG_DAMAGED",
                "OSD_SCRUB_ERRORS",
                "PG_DEGRADED",
            ]:
                check = checks.get(check_name, {})
                for detail in check.get("detail", []):
                    msg = detail.get("message", "")
                    # Extract PG ID from messages like "pg 3.a is ..."
                    parts = msg.split()
                    for i, p in enumerate(parts):
                        if p == "pg" and i + 1 < len(parts):
                            damaged_pgs.add(parts[i + 1])

            # Also find via rados list-inconsistent-pg
            pools = self.rados_obj.list_pools()
            for pool in pools:
                try:
                    inc_pgs = self.rados_obj.get_inconsistent_pg_list(pool)
                    if inc_pgs:
                        damaged_pgs.update(str(pg) for pg in inc_pgs)
                except Exception:
                    pass

            if damaged_pgs:
                max_repair = 50
                log.info(
                    f"Found {len(damaged_pgs)} damaged PG(s), "
                    f"repairing up to {max_repair}"
                )
                for pgid in list(damaged_pgs)[:max_repair]:
                    try:
                        self.rados_obj.run_ceph_command(f"ceph pg repair {pgid}")
                        result["pgs_repaired"] += 1
                        log.info(f"  Repaired PG {pgid}")
                    except Exception as e:
                        log.warning(f"  Failed to repair PG {pgid}: {e}")
                time.sleep(30)
            else:
                log.info("No inconsistent PGs found")
        except Exception as e:
            log.warning(f"PG repair step failed: {e}")

        # Step 2: Handle unfound objects
        log.info("Step 2: Handling unfound objects")
        try:
            health_out, _ = self.rados_obj.node.shell(["ceph health detail"])
            if "OBJECT_UNFOUND" in str(health_out):
                # Find PGs with unfound objects
                pg_dump = self.rados_obj.run_ceph_command("ceph pg dump_stuck unfound")
                unfound_pgs = []
                if isinstance(pg_dump, list):
                    unfound_pgs = [p.get("pgid") for p in pg_dump if p.get("pgid")]
                elif isinstance(pg_dump, dict):
                    unfound_pgs = [
                        p.get("pgid")
                        for p in pg_dump.get("pg_stats", [])
                        if "unfound" in p.get("state", "")
                    ]

                for pgid in unfound_pgs:
                    try:
                        self.rados_obj.node.shell(
                            [f"ceph pg {pgid} " f"mark_unfound_lost delete"]
                        )
                        result["objects_lost"] += 1
                        log.info(f"  Marked unfound objects in PG {pgid} " f"as lost")
                    except Exception as e:
                        log.warning(f"  Failed to handle unfound in " f"PG {pgid}: {e}")
                time.sleep(15)
            else:
                log.info("No unfound objects")
        except Exception as e:
            log.warning(f"Unfound objects step failed: {e}")

        # Step 3: Deep-scrub all OSDs
        log.info("Step 3: Running deep-scrub on all OSDs")
        try:
            self.rados_obj.run_deep_scrub()
            result["deep_scrub_run"] = True
            log.info("Deep-scrub initiated on all OSDs")
            time.sleep(30)
        except Exception as e:
            log.warning(f"Deep-scrub failed: {e}")

        # Step 4: Wait for PGs to reach active+clean
        log.info(f"Step 4: Waiting up to {timeout}s for active+clean PGs")
        try:
            deadline = time.time() + timeout
            while time.time() < deadline:
                health_out, _ = self.rados_obj.node.shell(["ceph health detail"])
                h = str(health_out)
                if (
                    "PG_DAMAGED" not in h
                    and "PG_DEGRADED" not in h
                    and "OBJECT_UNFOUND" not in h
                    and "OSD_SCRUB_ERRORS" not in h
                ):
                    result["health_ok"] = True
                    log.info("PG damage resolved")
                    break
                time.sleep(30)
            if not result["health_ok"]:
                log.warning("PG damage not fully resolved within timeout")
        except Exception as e:
            log.warning(f"Health check failed: {e}")

        # Step 5: Restart OSDs to clear spurious read error and repair counters
        try:
            if self.rados_obj.check_health_warning(
                "BLUESTORE_SPURIOUS_READ_ERRORS"
            ) or self.rados_obj.check_health_warning("OSD_TOO_MANY_REPAIRS"):
                log.info(
                    "Restarting OSD service to clear spurious read "
                    "error and repair counters"
                )
                try:
                    self.rados_obj.restart_daemon_services(daemon="osd")
                    log.info("OSD service restart completed")
                except Exception as e:
                    log.warning(f"OSD service restart failed: {e}")
        except Exception:
            pass

        result["summary"] = (
            f"BlueStore recovery: {result['pgs_repaired']} PGs repaired, "
            f"{result['objects_lost']} unfound objects marked lost, "
            f"deep_scrub={'yes' if result['deep_scrub_run'] else 'no'}, "
            f"health_ok={result['health_ok']}"
        )
        log.info(f"=== {result['summary']} ===")
        return result

    def recover_mds_corruption(self, fs_name, cephfs_mount_path=None, timeout=240):
        """
        Full customer recovery workflow for MDS corruption.

        Steps: mds repaired -> damage ls -> scrub repair -> damage rm -> verify.
        Diagnostic only -- logs results, does not affect test pass/fail.
        """
        mds_target = f"{fs_name}:0"
        result = {
            "rank_repaired": False,
            "mds_active": False,
            "damage_entries": [],
            "scrub_attempted": False,
            "damage_cleared": False,
            "fs_accessible": False,
            "new_crashes": 0,
            "outcome": "NOT_STARTED",
        }

        log.info(f"=== MDS CORRUPTION REPAIR WORKFLOW for {fs_name} ===")
        all_crashes = self.rados_obj.do_crash_ls() or []
        pre_crashes = len(
            [c for c in all_crashes if c.get("entity_name", "").startswith("mds.")]
        )

        # Step 1: Reset damaged rank
        log.info("Step 1: ceph mds repaired")
        try:
            self.rados_obj.node.shell([f"ceph mds repaired {mds_target}"])
            result["rank_repaired"] = True
        except Exception as e:
            log.warning(f"Repair command failed: {e}")
            result["outcome"] = "REPAIR_CMD_FAILED"
            return result

        deadline = time.time() + timeout
        while time.time() < deadline:
            post_all = self.rados_obj.do_crash_ls() or []
            post_crashes = len(
                [c for c in post_all if c.get("entity_name", "").startswith("mds.")]
            )
            if post_crashes > pre_crashes:
                result["new_crashes"] = post_crashes - pre_crashes
                result["outcome"] = "REPAIR_FAILED_MDS_CRASHED"
                log.warning(
                    f"MDS crashed {result['new_crashes']}x after "
                    f"repair -- corruption persists"
                )
                return result
            try:
                fs_data = self.rados_obj.run_ceph_command(f"ceph fs status {fs_name}")
                for m in fs_data.get("mdsmap", []):
                    if m.get("state") == "active":
                        result["mds_active"] = True
                        log.info(f"MDS {m.get('name')} active for {fs_name}")
                        break
                if result["mds_active"]:
                    break
            except Exception:
                pass
            time.sleep(15)

        if not result["mds_active"]:
            result["outcome"] = "REPAIR_FAILED_NO_ACTIVE"
            log.warning(f"No MDS became active within {timeout}s")
            return result

        # Step 2: Inspect damage table
        log.info("Step 2: damage ls")
        try:
            dmg = self.rados_obj.run_ceph_command(
                f"ceph tell mds.{mds_target} damage ls"
            )
            result["damage_entries"] = dmg or []
            log.info(f"Damage entries: {len(result['damage_entries'])}")
            for e in result["damage_entries"]:
                log.info(f"  {e}")
        except Exception as e:
            log.warning(f"damage ls failed: {e}")

        # Step 3: Scrub + repair
        log.info("Step 3: scrub start / recursive,repair,force")
        try:
            result["scrub_attempted"] = True
            self.rados_obj.node.shell(
                [f"ceph tell mds.{mds_target} " f"scrub start / recursive,repair,force"]
            )
            scrub_end = time.time() + 120
            while time.time() < scrub_end:
                try:
                    st = self.rados_obj.run_ceph_command(
                        f"ceph tell mds.{mds_target} scrub status"
                    )
                    s = str(st)
                    log.debug(f"Scrub status: {s[:200]}")
                    if "no active" in s.lower() or "0 inodes" in s:
                        break
                except Exception:
                    pass
                time.sleep(15)
            log.info("Scrub completed or timed out")
        except Exception as e:
            log.warning(f"Scrub failed: {e}")

        # Step 4: Check remaining damage
        log.info("Step 4: Check remaining damage")
        try:
            remaining = (
                self.rados_obj.run_ceph_command(f"ceph tell mds.{mds_target} damage ls")
                or []
            )
            if not remaining:
                result["damage_cleared"] = True
                log.info("Damage table empty -- scrub repaired all")
            else:
                log.warning(f"{len(remaining)} entries remain, " f"removing manually")
                for entry in remaining:
                    did = entry.get("id")
                    if did is None:
                        did = entry.get("damage_id")
                    if did is not None:
                        try:
                            self.rados_obj.node.shell(
                                [f"ceph tell mds.{mds_target} " f"damage rm {did}"]
                            )
                        except Exception:
                            pass
                final = self.rados_obj.run_ceph_command(
                    f"ceph tell mds.{mds_target} damage ls"
                )
                result["damage_cleared"] = isinstance(final, list) and len(final) == 0
        except Exception as e:
            log.warning(f"Damage check failed: {e}")

        # Step 5: Verify recovery
        log.info("Step 5: Verify recovery")
        try:
            h, _ = self.rados_obj.node.shell(["ceph health detail"])
            h_str = str(h)
            mds_warns = [
                w for w in ["MDS_DAMAGE", "MDS_ALL_DOWN", "FS_DEGRADED"] if w in h_str
            ]
            if not mds_warns:
                log.info("No MDS health warnings")
            else:
                log.warning(f"MDS health warnings remain: {mds_warns}")
            if cephfs_mount_path and self.client_node:
                self.client_node.exec_command(
                    cmd=f"mkdir -p {cephfs_mount_path}",
                    sudo=True,
                    check_ec=False,
                )
                self.client_node.exec_command(
                    cmd=f"timeout 30 mount -t ceph "
                    f"admin@.{fs_name}=/ {cephfs_mount_path}",
                    sudo=True,
                    check_ec=False,
                )
                out, _ = self.client_node.exec_command(
                    cmd=f"timeout 10 ls {cephfs_mount_path}",
                    sudo=True,
                    check_ec=False,
                )
                if out is not None and "Transport endpoint" not in str(out):
                    result["fs_accessible"] = True
                    log.info("Filesystem accessible after repair")
                else:
                    log.warning("Filesystem not accessible")
                self.client_node.exec_command(
                    cmd=f"umount -l {cephfs_mount_path}",
                    sudo=True,
                    check_ec=False,
                )
            else:
                log.info("Skipping mount verification")
        except Exception as e:
            log.warning(f"Recovery verification failed: {e}")

        # Determine outcome
        if result["fs_accessible"] and result["damage_cleared"]:
            result["outcome"] = "FULL_REPAIR_SUCCEEDED"
        elif result["mds_active"] and result["damage_cleared"]:
            result["outcome"] = "PARTIAL_REPAIR_FS_NOT_ACCESSIBLE"
        elif result["mds_active"] and result["fs_accessible"]:
            result["outcome"] = "PARTIAL_REPAIR_DAMAGE_REMAINS"
        elif result["mds_active"]:
            result["outcome"] = "PARTIAL_REPAIR"
        else:
            result["outcome"] = "REPAIR_FAILED"

        log.info(
            f"=== REPAIR OUTCOME: {result['outcome']} | "
            f"active={result['mds_active']} "
            f"damage_cleared={result['damage_cleared']} "
            f"accessible={result['fs_accessible']} "
            f"new_crashes={result['new_crashes']} ==="
        )
        return result

    def destroy_damaged_filesystem(self, fs_name, cephfs_mount_path=None):
        """
        Destroy a damaged filesystem and its pools.
        Used when MDS corruption makes the FS unrecoverable.
        """
        log.info(f"Destroying damaged filesystem '{fs_name}'")
        try:
            if cephfs_mount_path and self.client_node:
                self.client_node.exec_command(
                    cmd=f"umount -l {cephfs_mount_path}",
                    sudo=True,
                    check_ec=False,
                )
                time.sleep(3)
            self.rados_obj.node.shell([f"ceph fs fail {fs_name}"])
            time.sleep(5)
            self.rados_obj.node.shell([f"ceph fs rm {fs_name} --yes-i-really-mean-it"])
            time.sleep(5)
            for pool in [
                f"cephfs_erasure_{fs_name}_metadata",
                f"cephfs_erasure_{fs_name}_data",
            ]:
                try:
                    self.rados_obj.delete_pool(pool=pool)
                except Exception:
                    pass
            self.rados_obj.node.shell(["ceph crash archive-all"])
            log.info("Damaged filesystem and pools removed")
            time.sleep(15)
        except Exception as e:
            log.warning(f"FS destruction failed: {e}")

    def recover_all(self, fs_name=None, cephfs_mount_path=None):
        """
        Auto-detect and recover from all injection damage.
        Checks ceph health detail and runs appropriate recovery.
        """
        log.info("=== AUTO-DETECT INJECTION RECOVERY ===")
        result = {
            "bluestore_recovery": None,
            "mds_recovery": None,
        }

        try:
            health_out, _ = self.rados_obj.node.shell(["ceph health detail"])
            health = str(health_out)
        except Exception as e:
            log.warning(f"Cannot read cluster health: {e}")
            return result

        bs_indicators = [
            "BLUESTORE_SPURIOUS_READ_ERRORS",
            "OSD_SCRUB_ERRORS",
            "PG_DAMAGED",
            "OBJECT_UNFOUND",
            "OSD_TOO_MANY_REPAIRS",
        ]
        if any(ind in health for ind in bs_indicators):
            log.info("BlueStore damage detected, running recovery")
            result["bluestore_recovery"] = self.recover_bluestore_damage()

        mds_indicators = [
            "MDS_DAMAGE",
            "MDS_ALL_DOWN",
            "FS_DEGRADED",
        ]
        if any(ind in health for ind in mds_indicators):
            if fs_name:
                log.info("MDS damage detected, running recovery")
                result["mds_recovery"] = self.recover_mds_corruption(
                    fs_name, cephfs_mount_path
                )

        if not result["bluestore_recovery"] and not result["mds_recovery"]:
            log.info("No injection damage detected")

        return result
