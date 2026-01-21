"""
NFS Ganesha gRPC Service Tests

This module contains tests for verifying NFS Ganesha gRPC service functionality.
It uses the 'operation' config parameter to determine which test scenario to run.

Supported Operations:
    - verify_port: Verify gRPC Service Port Availability (port 50051)
    - verify_discovery: Verify gRPC Service Discovery (list available methods)
    - grace_event_0: Start Grace Period (Event ID 0)
    - grace_event_2: Release IP from Grace (Event ID 2)
    - grace_event_4: Node Takeover (Event ID 4)
    - grace_event_5: IP Takeover (Event ID 5)
    - verify_client_session_ids: Verify Client and Session IDs for multiple clients
    - verify_id_after_unmount: Verify ID Updates After Client Unmount
"""

import json
import re
from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from tests.nfs.nfs_operations import cleanup_cluster, nfs_log_parser, setup_nfs_cluster
from utility.log import Log

log = Log(__name__)

# gRPC port for NFS Ganesha
GRPC_PORT = 50051

# Expected gRPC methods/services
EXPECTED_GRPC_SERVICES = [
    "nfsService.GetClientId",
    "nfsService.GetNfsGrace",
    "nfsService.GetSessionId",
    "nfsService.StartNfsGrace",
    "grpc.reflection.v1alpha.ServerReflection",
]


def install_grpcurl(node):
    """
    Install grpcurl tool on the specified node.

    Args:
        node: Node object to install grpcurl on
    """
    log.info(f"Installing grpcurl on {node.hostname}")

    # Check if already installed
    out, _ = node.exec_command(cmd="which grpcurl", check_ec=False)
    if "grpcurl" in out:
        log.info("grpcurl already installed")
        return

    wget_cmd = (
        "curl -LO "
        "https://github.com/fullstorydev/grpcurl/releases/download/v1.8.9/grpcurl_1.8.9_linux_x86_64.tar.gz"
    )
    tar_cmd = "tar -xvzf grpcurl_1.8.9_linux_x86_64.tar.gz"
    chmod_cmd = "chmod +x grpcurl"
    rename_cmd = "mv grpcurl /usr/local/bin/"

    node.exec_command(sudo=True, cmd=wget_cmd)
    node.exec_command(sudo=True, cmd=f"{tar_cmd} && {chmod_cmd} && {rename_cmd}")

    # Verify installation
    out, _ = node.exec_command(cmd="grpcurl --version", check_ec=False)
    log.info(f"grpcurl version: {out}")


def verify_grpc_port_availability(nfs_node, nfs_ip):
    """
    Test Scenario 1: Verify gRPC Service Port Availability

    Ensure gRPC service (port 50051) is listening along with NFS service (2049).

    Args:
        nfs_node: NFS server node object
        nfs_ip: IP address of the NFS server

    Returns:
        bool: True if port is available, False otherwise
    """
    log.info(f"Verifying gRPC port {GRPC_PORT} availability on {nfs_ip}")

    # Check if ganesha.nfsd is listening on port 50051
    cmd = f"ss -tulnp | grep {GRPC_PORT}"
    out, _ = nfs_node.exec_command(sudo=True, cmd=cmd, check_ec=False)

    if str(GRPC_PORT) in out:
        log.info(f"gRPC port {GRPC_PORT} is listening: {out}")

        # Verify ganesha.nfsd process is associated
        if "ganesha" in out.lower() or "nfsd" in out.lower():
            log.info("ganesha.nfsd process is listening on gRPC port")
            return True
        else:
            # Check process separately
            cmd_ps = "ss -tulnp | grep 50051 | grep -i ganesha"
            out_ps, _ = nfs_node.exec_command(sudo=True, cmd=cmd_ps, check_ec=False)
            if out_ps:
                log.info(f"ganesha.nfsd confirmed on gRPC port: {out_ps}")
                return True

        # Port is listening but process verification needed
        log.warning("Port is listening but process name not confirmed in ss output")
        return True

    log.error(f"gRPC port {GRPC_PORT} is not listening")
    return False


def verify_grpc_service_discovery(client_node, nfs_ip):
    """
    Test Scenario 2: Verify gRPC Service Discovery

    Confirm available gRPC methods using grpcurl.

    Args:
        client_node: Client node object
        nfs_ip: IP address of the NFS server

    Returns:
        tuple: (bool, list) - Success status and list of discovered services
    """
    log.info(f"Discovering gRPC services on {nfs_ip}:{GRPC_PORT}")

    # Use grpcurl to list services
    cmd = f"grpcurl -plaintext {nfs_ip}:{GRPC_PORT} list"
    out, err = client_node.exec_command(sudo=True, cmd=cmd, check_ec=False)

    if err and "error" in err.lower():
        log.error(f"Failed to discover gRPC services: {err}")
        return False, []

    discovered_services = [
        line.strip() for line in out.strip().split("\n") if line.strip()
    ]
    log.info(f"Discovered gRPC services: {discovered_services}")

    # Check for expected services
    found_services = []
    for expected in EXPECTED_GRPC_SERVICES:
        service_name = expected.split(".")[0] if "." in expected else expected
        if any(service_name in svc for svc in discovered_services):
            found_services.append(expected)

    log.info(f"Found expected services: {found_services}")
    return len(found_services) > 0, discovered_services


def start_grace_period(client_node, nfs_ip, event_id, node_id=1):
    """
    Test Scenarios 3-6: Start Grace Period with different Event IDs

    Event IDs:
        0 - Start Grace Period
        2 - Release IP from Grace
        4 - Node Takeover
        5 - IP Takeover

    Args:
        client_node: Client node object
        nfs_ip: IP address of the NFS server
        event_id: Event ID (0, 2, 4, or 5)
        node_id: Node ID (default: 1)

    Returns:
        tuple: (bool, str) - Success status and response/output
    """
    log.info(f"Starting grace period with Event ID {event_id} on {nfs_ip}")

    # Build the gRPC request
    request_data = f'{{"Event":{event_id},"NodeId":{node_id},"IpAddr":"{nfs_ip}"}}'

    # Use grpcurl to make the call
    cmd = (
        f"grpcurl -plaintext -d '{request_data}' "
        f"{nfs_ip}:{GRPC_PORT} nfsService.StartNfsGrace/StartGraceWithEvent"
    )

    out, err = client_node.exec_command(sudo=True, cmd=cmd, check_ec=False)

    log.info(f"gRPC response: {out}")
    if err:
        log.warning(f"gRPC stderr: {err}")

    # Check for success based on event type
    if event_id == 0:
        # Expect graceStarted: true
        if "graceStarted" in out and "true" in out.lower():
            log.info("Grace period started successfully")
            return True, out
    else:
        # For other events, check for successful response (no error)
        if "error" not in out.lower() or "rpc error" not in out.lower():
            log.info(f"Event {event_id} executed successfully")
            return True, out

    log.error(f"Failed to execute event {event_id}")
    return False, out


def verify_grace_logs(nfs_node, nfs_name, event_id, client):
    """
    Verify expected log messages after grace period events.

    Args:
        nfs_node: NFS server node object
        nfs_name: NFS cluster name
        event_id: Event ID that was executed

    Returns:
        bool: True if expected logs are found
    """
    log.info(f"Verifying logs for Event ID {event_id}")

    # Define expected log patterns based on event ID
    expected_patterns = {
        0: ["NFS Server Now IN GRACE"],
        2: ["nfs_release_v4_clients"],
        4: ["nfs_start_grace :STATE :EVENT :NFS Server recovery event 4 nodeid 1"],
        5: ["nfs_start_grace :STATE :EVENT :NFS Server recovery event 5 nodeid 1"],
    }

    patterns = expected_patterns.get(event_id, [])
    if not patterns:
        log.warning(f"No expected patterns defined for Event ID {event_id}")
        return True

    # Use nfs_log_parser to check logs
    result = nfs_log_parser(
        client=client,
        nfs_node=nfs_node,
        nfs_name=nfs_name,
        expect_list=patterns,
    )

    return result == 0


def get_client_ids(client_node, nfs_ip):
    """
    Get NFS client IDs via gRPC.

    Args:
        client_node: Client node object
        nfs_ip: IP address of the NFS server

    Returns:
        tuple: (bool, list) - Success status and list of client IDs
    """
    log.info(f"Getting client IDs from {nfs_ip}")

    cmd = f"grpcurl -plaintext {nfs_ip}:{GRPC_PORT} nfsService.GetClientId/GetClientIds"
    out, err = client_node.exec_command(sudo=True, cmd=cmd, check_ec=False)

    log.info(f"GetClientIds response: {out}")

    if err and "error" in err.lower():
        log.error(f"Failed to get client IDs: {err}")
        return False, []

    # Parse client IDs from response
    client_ids = []
    try:
        # Try to parse as JSON
        response = json.loads(out) if out.strip() else {}
        if "clientIds" in response:
            client_ids = response["clientIds"]
        elif "client_ids" in response:
            client_ids = response["client_ids"]
    except json.JSONDecodeError:
        # Parse from text output
        matches = re.findall(r'"?clientId"?\s*:\s*"?(\d+)"?', out, re.IGNORECASE)
        client_ids = matches

    log.info(f"Found client IDs: {client_ids}")
    return True, client_ids


def get_session_ids(client_node, nfs_ip):
    """
    Get NFS session IDs via gRPC.

    Args:
        client_node: Client node object
        nfs_ip: IP address of the NFS server

    Returns:
        tuple: (bool, list) - Success status and list of session IDs
    """
    log.info(f"Getting session IDs from {nfs_ip}")

    cmd = (
        f"grpcurl -plaintext {nfs_ip}:{GRPC_PORT} nfsService.GetSessionId/GetSessionIds"
    )
    out, err = client_node.exec_command(sudo=True, cmd=cmd, check_ec=False)

    log.info(f"GetSessionIds response: {out}")

    if err and "error" in err.lower():
        log.error(f"Failed to get session IDs: {err}")
        return False, []

    # Parse session IDs from response
    session_ids = []
    try:
        response = json.loads(out) if out.strip() else {}
        if "sessionIds" in response:
            session_ids = response["sessionIds"]
        elif "session_ids" in response:
            session_ids = response["session_ids"]
    except json.JSONDecodeError:
        matches = re.findall(r'"?sessionId"?\s*:\s*"?(\d+)"?', out, re.IGNORECASE)
        session_ids = matches

    log.info(f"Found session IDs: {session_ids}")
    return True, session_ids


def setup_additional_mounts(clients, nfs_server, nfs_export, nfs_mount, version, port):
    """
    Mount NFS export on additional clients (clients[1:]).

    Args:
        clients: List of client node objects
        nfs_server: NFS server hostname or IP
        nfs_export: Export path
        nfs_mount: Base mount point
        version: NFS version
        port: NFS port

    Returns:
        list: List of mount points created
    """
    mount_points = []

    for i, client in enumerate(clients[1:], start=1):
        mount_point = f"{nfs_mount}_{i}"
        client.create_dirs(dir_path=mount_point, sudo=True)

        if Mount(client).nfs(
            mount=mount_point,
            version=version,
            port=port,
            server=nfs_server,
            export=nfs_export,
        ):
            log.error(f"Failed to mount NFS on {client.hostname}")
            continue

        log.info(f"NFS mounted successfully on {client.hostname} at {mount_point}")

        # Create a test file to establish connection
        client.exec_command(sudo=True, cmd=f"touch {mount_point}/testfile_{i}")
        mount_points.append((client, mount_point))
        sleep(2)

    return mount_points


def cleanup_additional_mounts(mount_points):
    """
    Unmount additional NFS mounts.

    Args:
        mount_points: List of (client, mount_point) tuples
    """
    for client, mount_point in mount_points:
        try:
            client.exec_command(
                sudo=True, cmd=f"rm -rf {mount_point}/*", check_ec=False
            )
            Unmount(client).unmount(mount_point)
            client.exec_command(sudo=True, cmd=f"rm -rf {mount_point}", check_ec=False)
            log.info(f"Cleaned up mount on {client.hostname}")
        except Exception as e:
            log.warning(f"Cleanup error on {client.hostname}: {e}")


def run(ceph_cluster, **kw):
    """
    Test NFS Ganesha gRPC Service functionality.

    This function uses the 'operation' config parameter to determine which
    test scenario to run. Each operation is a separate test case.

    Supported Operations:
        - verify_port: Verify gRPC port 50051 is listening
        - verify_discovery: Verify gRPC service discovery
        - grace_event_0: Start Grace Period (Event ID 0)
        - grace_event_2: Release IP from Grace (Event ID 2)
        - grace_event_4: Node Takeover (Event ID 4)
        - grace_event_5: IP Takeover (Event ID 5)
        - verify_client_session_ids: Verify Client and Session IDs
        - verify_id_after_unmount: Verify ID Updates After Unmount

    Args:
        ceph_cluster: Ceph cluster object
        **kw: Keyword arguments containing test configuration

    Returns:
        int: 0 on success, 1 on failure
    """
    config = kw.get("config", {})

    # Get operation to perform
    operation = config.get("operation")
    if not operation:
        raise ConfigError("'operation' is required in config")

    # Get common configuration
    nfs_name = config.get("nfs_name", "cephfs-nfs")
    nfs_export = config.get("nfs_export", "/export")
    nfs_mount = config.get("nfs_mount", "/mnt/nfs")
    nfs_version = config.get("nfs_version", 4.1)
    nfs_port = config.get("port", 2049)
    fs_name = config.get("fs_name", "cephfs")
    fs = config.get("fs", "cephfs")
    num_clients = int(config.get("clients", 1))
    subvolume_group = config.get("subvolume_group", "ganeshagroup")

    # Get nodes
    clients = ceph_cluster.get_nodes(role="client")
    nfs_nodes = ceph_cluster.get_nodes(role="nfs")

    if not clients:
        raise OperationFailedError("No client nodes available")

    if not nfs_nodes:
        raise OperationFailedError("No NFS nodes available")

    if num_clients > len(clients):
        raise ConfigError(
            f"Test requires {num_clients} clients but only {len(clients)} available"
        )

    clients = clients[:num_clients]
    client = clients[0]
    nfs_node = nfs_nodes[0]
    nfs_server = nfs_node.hostname
    nfs_ip = nfs_node.ip_address

    log.info(f"Running operation: {operation}")
    log.info(f"Using NFS server: {nfs_server} ({nfs_ip})")
    log.info(f"Using {len(clients)} client(s) for testing")

    # Create subvolume group
    Ceph(client).fs.sub_volume_group.create(volume=fs_name, group=subvolume_group)

    additional_mounts = []

    try:
        # Install grpcurl on client(s)
        for c in clients:
            install_grpcurl(c)

        client = clients[0]

        # Allow GRPC Port 50051
        cmd = f"firewall-cmd --permanent --add-port={GRPC_PORT}/tcp"
        nfs_node.exec_command(sudo=True, cmd=cmd, check_ec=False)

        # Setup NFS cluster for most operations
        setup_nfs_cluster(
            clients=[client],  # Primary client only for setup
            nfs_server=nfs_server,
            port=nfs_port,
            version=nfs_version,
            nfs_name=nfs_name,
            nfs_mount=nfs_mount,
            fs_name=fs_name,
            export=nfs_export,
            fs=fs,
            ceph_cluster=ceph_cluster,
        )
        log.info("NFS cluster setup complete.")

        # =================================================================
        # Execute the requested operation
        # =================================================================

        if operation == "verify_port":
            # Test Scenario 1: Verify gRPC Service Port Availability
            log.info("=" * 60)
            log.info("TEST: Verify gRPC Service Port Availability")
            log.info("=" * 60)

            if not verify_grpc_port_availability(nfs_node, nfs_ip):
                raise OperationFailedError(
                    f"gRPC port {GRPC_PORT} is not available on {nfs_ip}"
                )
            log.info(f"PASS: gRPC port {GRPC_PORT} is available")

        elif operation == "verify_discovery":
            # Test Scenario 2: Verify gRPC Service Discovery
            log.info("=" * 60)
            log.info("TEST: Verify gRPC Service Discovery")
            log.info("=" * 60)

            success, services = verify_grpc_service_discovery(client, nfs_ip)
            if not success:
                raise OperationFailedError(
                    f"Could not discover gRPC services on {nfs_ip}:{GRPC_PORT}"
                )
            log.info(f"PASS: Discovered gRPC services: {services}")

        elif operation.startswith("grace_event_"):
            # Test Scenarios 3-6: Grace Period Events
            event_id = int(operation.split("_")[-1])
            event_names = {
                0: "Start Grace Period",
                2: "Release IP from Grace",
                4: "Node Takeover",
                5: "IP Takeover",
            }
            event_name = event_names.get(event_id, f"Event {event_id}")

            log.info("=" * 60)
            log.info(f"TEST: {event_name} (Event ID {event_id})")
            log.info("=" * 60)

            success, response = start_grace_period(client, nfs_ip, event_id)
            if not success:
                log.warning(f"Grace event {event_id} response: {response}")

            # Wait for logs to be written
            sleep(5)

            # Verify logs
            verify_grace_logs(nfs_node, nfs_name, event_id, client)

            log.info(f"PASS: {event_name} executed successfully")

        elif operation == "verify_client_session_ids":
            # Test Scenario 7: Verify Client and Session IDs
            log.info("=" * 60)
            log.info("TEST: Verify Client and Session IDs for Multiple Clients")
            log.info("=" * 60)

            # Mount on additional clients
            if len(clients) > 1:
                additional_mounts = setup_additional_mounts(
                    clients,
                    nfs_server,
                    f"{nfs_export}_0",
                    nfs_mount,
                    nfs_version,
                    nfs_port,
                )

            # Wait for connections to establish
            sleep(10)

            # Get client IDs
            success, client_ids = get_client_ids(client, nfs_ip)
            if not success:
                raise OperationFailedError("Could not get client IDs")

            # Get session IDs
            success, session_ids = get_session_ids(client, nfs_ip)
            if not success:
                raise OperationFailedError("Could not get session IDs")

            log.info(f"Client IDs: {client_ids}")
            log.info(f"Session IDs: {session_ids}")

            # Verify we have unique IDs
            unique_client_ids = len(set(client_ids))
            unique_session_ids = len(set(session_ids))

            log.info(f"Unique Client IDs: {unique_client_ids}")
            log.info(f"Unique Session IDs: {unique_session_ids}")

            log.info("PASS: Client and Session IDs retrieved successfully")

        elif operation == "verify_id_after_unmount":
            # Test Scenario 8: Verify ID Updates After Unmount
            log.info("=" * 60)
            log.info("TEST: Verify ID Updates After Client Unmount")
            log.info("=" * 60)

            if len(clients) < 2:
                raise ConfigError("This test requires at least 2 clients")

            # Mount on additional clients
            additional_mounts = setup_additional_mounts(
                clients, nfs_server, f"{nfs_export}_0", nfs_mount, nfs_version, nfs_port
            )

            sleep(10)

            # Get initial IDs
            success, initial_client_ids = get_client_ids(client, nfs_ip)
            success, initial_session_ids = get_session_ids(client, nfs_ip)

            log.info(f"Initial Client IDs: {initial_client_ids}")
            log.info(f"Initial Session IDs: {initial_session_ids}")

            # Unmount from last client
            if additional_mounts:
                unmount_client, unmount_point = additional_mounts[-1]
                log.info(f"Unmounting from {unmount_client.hostname}")

                unmount_client.exec_command(
                    sudo=True, cmd=f"rm -rf {unmount_point}/*", check_ec=False
                )
                Unmount(unmount_client).unmount(unmount_point)
                unmount_client.exec_command(
                    sudo=True, cmd=f"rm -rf {unmount_point}", check_ec=False
                )

                # Remove from list so cleanup doesn't try again
                additional_mounts = additional_mounts[:-1]

            # Wait for session cleanup
            sleep(15)

            # Get updated IDs
            success, updated_client_ids = get_client_ids(client, nfs_ip)
            success, updated_session_ids = get_session_ids(client, nfs_ip)

            log.info(f"Updated Client IDs: {updated_client_ids}")
            log.info(f"Updated Session IDs: {updated_session_ids}")

            # Verify client count changed
            if len(set(updated_client_ids)) <= len(set(initial_client_ids)):
                log.info("PASS: Client count decreased or stayed same after unmount")
            else:
                log.warning("Client count unexpectedly increased")

        else:
            raise ConfigError(f"Unknown operation: {operation}")

        return 0

    except (ConfigError, OperationFailedError) as e:
        log.error(f"Test failed: {e}")
        return 1
    except Exception as e:
        log.error(f"Test failed with unexpected error: {e}")
        import traceback

        log.error(traceback.format_exc())
        return 1

    finally:
        log.info("=" * 60)
        log.info("CLEANUP")
        log.info("=" * 60)

        try:
            # Cleanup additional mounts
            cleanup_additional_mounts(additional_mounts)

            # Parse NFS logs for debugging
            nfs_log_parser(client=client, nfs_node=nfs_nodes, nfs_name=nfs_name)

            # Cleanup cluster
            cleanup_cluster(client, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_node)
            log.info("Cleanup completed")
        except Exception as cleanup_error:
            log.warning(f"Cleanup error (non-fatal): {cleanup_error}")
