"""
Test module for deploying OSDs in a rack-based CRUSH hierarchy.

This test creates a CRUSH hierarchy with multiple racks (A, B, C) and
distributes OSD hosts across these racks using 'ceph osd crush move' commands.

Rack assignment can be controlled via node configuration by adding 'rack' attribute
to each OSD node. If not specified, hosts are distributed using round-robin.
"""

from ceph.ceph_admin import CephAdmin
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Deploy OSDs in a rack-based CRUSH hierarchy.

    Args:
        ceph_cluster: Ceph cluster object
        **kw: Test configuration

    Test workflow:
        1. Get all OSD hosts
        2. Create rack buckets (A, B, C, etc.)
        3. Move racks under root=default
        4. Distribute hosts across racks based on node configuration
        5. Move hosts into their assigned racks using 'ceph osd crush move'
        6. Verify CRUSH tree structure

    Config parameters:
        racks: List of rack names (default: ["A", "B", "C"])
        verify_tree: Boolean to verify CRUSH tree (default: True)

    Node configuration (in cluster config file):
        Each OSD node can have a 'rack' attribute specifying its rack assignment.
        If not specified, round-robin distribution is used.

        Example:
            node4:
              role:
                - osd
              rack: A
              no-of-volumes: 12

    Returns:
        0 on success, 1 on failure

    Example suite config:
        config:
          racks: ["A", "B", "C"]
          verify_tree: true
    """
    log.info("Starting rack-based OSD deployment test")
    config = kw.get("config", {})

    # Get configuration parameters
    verify_tree = config.get("verify_tree", True)

    # Get admin node
    cephadm = CephAdmin(cluster=ceph_cluster, **config)

    try:
        # Step 1: Get all OSD hosts and detect racks from configuration
        log.info("Step 1: Getting all OSD hosts and detecting racks from configuration")

        osd_hosts = ceph_cluster.get_nodes(role="osd")
        if not osd_hosts:
            log.error("No OSD hosts found in the cluster")
            return 1

        log.info(f"Found {len(osd_hosts)} OSD hosts")

        # Auto-detect rack names from node configurations
        detected_racks = set()
        for host in osd_hosts:
            rack_name = getattr(host, 'rack', None)
            if rack_name:
                detected_racks.add(rack_name)
            log.info(f"  - {host.hostname} (rack: {rack_name if rack_name else 'not specified'})")

        # Use detected racks or fall back to config/default
        if detected_racks:
            rack_names = sorted(list(detected_racks))
            log.info(f"Auto-detected racks from configuration: {rack_names}")
        else:
            rack_names = config.get("racks", ["A", "B", "C"])
            log.info(f"No racks detected in configuration, using: {rack_names}")

        # Step 2: Create rack buckets
        log.info("Step 2: Creating rack buckets")

        for rack_name in rack_names:
            cmd = f"ceph osd crush add-bucket {rack_name} rack"
            log.info(f"Creating rack: {rack_name}")
            try:
                out, err = cephadm.shell([cmd])
                log.info(f"Rack {rack_name} created successfully")
                if out:
                    log.debug(f"Output: {out}")
            except Exception as e:
                # Rack might already exist
                if "already exists" in str(e).lower():
                    log.info(f"Rack {rack_name} already exists, continuing...")
                else:
                    log.error(f"Failed to create rack {rack_name}: {e}")
                    raise

        # Step 3: Move racks under root=default
        log.info("Step 3: Moving racks under root=default")

        for rack_name in rack_names:
            cmd = f"ceph osd crush move {rack_name} root=default"
            log.info(f"Moving rack {rack_name} to root=default")
            try:
                out, err = cephadm.shell([cmd])
                log.info(f"Rack {rack_name} moved to root=default")
                if out:
                    log.debug(f"Output: {out}")
            except Exception as e:
                log.warning(f"Could not move rack {rack_name}: {e}")

        # Step 4: Distribute hosts across racks
        log.info("Step 4: Distributing hosts across racks")

        rack_distribution = {rack: [] for rack in rack_names}

        for host in osd_hosts:
            hostname = host.hostname

            # Check if rack is specified in node configuration
            rack_name = getattr(host, 'rack', None)

            if not rack_name:
                # Fallback to round-robin distribution if rack not specified
                log.warning(f"No rack specified for {hostname}, using round-robin distribution")
                index = osd_hosts.index(host)
                rack_index = index % len(rack_names)
                rack_name = rack_names[rack_index]

            # Validate rack name
            if rack_name not in rack_names:
                log.error(f"Invalid rack '{rack_name}' specified for {hostname}. Valid racks: {rack_names}")
                return 1

            rack_distribution[rack_name].append(hostname)

            cmd = f"ceph osd crush move {hostname} rack={rack_name}"
            log.info(f"Moving host {hostname} to rack {rack_name}")

            try:
                out, err = cephadm.shell([cmd])
                log.info(f"✓ Host {hostname} successfully moved to rack {rack_name}")
                if out:
                    log.debug(f"Output: {out}")
            except Exception as e:
                log.error(f"✗ Failed to move host {hostname} to rack {rack_name}: {e}")
                return 1

        # Log distribution summary
        log.info("Rack distribution summary:")
        for rack_name, hosts in rack_distribution.items():
            log.info(f"  Rack {rack_name}: {len(hosts)} hosts")
            for hostname in hosts:
                log.info(f"    - {hostname}")

        # Step 5: Verify CRUSH tree structure
        if verify_tree:
            log.info("Step 5: Verifying CRUSH tree structure")

            cmd = "ceph osd tree"
            out, err = cephadm.shell([cmd])
            log.info("Current CRUSH tree:")
            log.info("\n" + out)

            # Verify each rack exists in the tree
            verification_passed = True
            for rack_name in rack_names:
                if f"rack {rack_name}" in out or f"rack{rack_name}" in out:
                    log.info(f"✓ Rack {rack_name} found in CRUSH tree")
                else:
                    log.error(f"✗ Rack {rack_name} NOT found in CRUSH tree")
                    verification_passed = False

            # Verify hosts are in the tree
            for host in osd_hosts:
                if host.hostname in out:
                    log.info(f"✓ Host {host.hostname} found in CRUSH tree")
                else:
                    log.error(f"✗ Host {host.hostname} NOT found in CRUSH tree")
                    verification_passed = False

            if not verification_passed:
                log.error("CRUSH tree verification failed")
                return 1

            log.info("✓ CRUSH tree verification passed")

        # Step 6: Verify cluster health
        log.info("Step 6: Verifying cluster health")

        cmd = "ceph -s"
        out, err = cephadm.shell([cmd])
        log.info("Cluster status:")
        log.info("\n" + out)

        log.info("✓ Rack-based OSD deployment test completed successfully")

        return 0

    except Exception as e:
        log.error(f"✗ Test failed with exception: {e}")
        log.error("Exception details:", exc_info=True)
        return 1

# Made with Bob
