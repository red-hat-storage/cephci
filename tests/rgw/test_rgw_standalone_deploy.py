"""
Deploy IBM Ceph RGW Standalone (Zipper) container image via podman.

Writes endpoint metadata to /tmp/rgw_standalone_endpoint.json on the target
node so sanity_rgw.py (use-standalone: true) can inject the S3 endpoint into
pytest configs.

Suite config example::

    - test:
        module: test_rgw_standalone_deploy.py
        config:
          image: cp.stg.icr.io/cp/ibm-ceph/rgw-standalone-rhel10:v9.9.2
          container-name: rgw-standalone1
          host-port: 7401
          # optional developer-experience ports:
          # browser-port: 8081
          # metrics-port: 9080
"""

import json
import time

from utility.log import Log
from utility.utils import get_cephci_config

log = Log(__name__)

ENDPOINT_META = "/tmp/rgw_standalone_endpoint.json"
DEFAULT_IMAGE = "cp.stg.icr.io/cp/ibm-ceph/rgw-standalone-rhel10:v9.9.2"
DEFAULT_NAME = "rgw-standalone1"
DEFAULT_HOST_PORT = 7401
CONTAINER_S3_PORT = 7480
DATA_DIR = "/root/rgw_standalone_data"
POSIX_DIR = "/root/rgw_standalone_posix"


def _detect_tier(registry_host, build_type):
    if "preprod.icr.io" in registry_host:
        return "preprod"
    if "cp.icr.io" in registry_host and "stg" not in registry_host:
        return "cdn"
    if "stg" in registry_host or "stage" in registry_host:
        return "stage"
    return "cdn" if build_type in ("released", "cdn") else "stage"


def _registry_login(node, image, product, build_type):
    """Login to the image registry using cephci.yaml credentials."""
    registry_host = image.split("/")[0]
    cfg = get_cephci_config()
    vendor = "ibm" if "ibm" in (product or "ibm") else "rh"
    tier = _detect_tier(registry_host, build_type or "stage")
    cred = (
        cfg.get("credentials", {})
        .get("registry", {})
        .get(vendor, {})
        .get(tier)
        or cfg.get(f"{vendor}_registry_credentials")
        or cfg.get("cdn_credentials")
        or {}
    )
    user = cred.get("username", "")
    password = cred.get("password", "")
    if not user or not password:
        log.warning(
            "No registry credentials found; assuming image is already pulled/cached"
        )
        return

    log.info(f"Logging into registry {registry_host} (tier={tier})")
    node.exec_command(
        sudo=True,
        cmd=f"podman login -u '{user}' -p '{password}' {registry_host}",
        check_ec=False,
    )


def _wait_for_endpoint(node, endpoint, timeout=120):
    """Poll S3 endpoint until it answers (any HTTP status)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        out, _ = node.exec_command(
            cmd=(
                f"curl -sS -o /dev/null -w '%{{http_code}}' "
                f"--connect-timeout 3 {endpoint}/ || true"
            ),
            check_ec=False,
        )
        code = (out or "").strip()
        if code and code != "000":
            log.info(f"Zipper endpoint ready: {endpoint} (http={code})")
            return True
        time.sleep(3)
    return False


def run(ceph_cluster, **kw):
    config = kw.get("config", {})
    image = config.get("image", DEFAULT_IMAGE)
    name = config.get("container-name", DEFAULT_NAME)
    host_port = int(config.get("host-port", DEFAULT_HOST_PORT))
    browser_port = config.get("browser-port")
    metrics_port = config.get("metrics-port")
    data_dir = config.get("data-dir", DATA_DIR)
    posix_dir = config.get("posix-dir", POSIX_DIR)
    product = config.get("product", "ibm")
    build_type = config.get("build_type", config.get("build-type", "stage"))
    force_recreate = config.get("force-recreate", True)

    # Prefer client node; fall back to installer / rgw role
    target = (
        ceph_cluster.get_ceph_object("client")
        or ceph_cluster.get_ceph_object("rgw")
        or ceph_cluster.get_ceph_object("installer")
    )
    if not target:
        log.error("No client/rgw/installer node available for zipper deploy")
        return 1
    node = target.node

    log.info(f"Deploying RGW Standalone on {node.hostname}: {image}")
    node.exec_command(sudo=True, cmd="which podman || yum install -y podman", check_ec=False)
    _registry_login(node, image, product, build_type)

    node.exec_command(sudo=True, cmd=f"mkdir -p {data_dir} {posix_dir}")

    if force_recreate:
        node.exec_command(
            sudo=True,
            cmd=f"podman rm -f {name} 2>/dev/null || true",
            check_ec=False,
        )

    # Check if already running
    out, _ = node.exec_command(
        sudo=True,
        cmd=f"podman ps --filter name=^{name}$ --format '{{{{.Names}}}}'",
        check_ec=False,
    )
    if (out or "").strip() == name:
        log.info(f"Container {name} already running; reusing")
    else:
        ports = [f"-p {host_port}:{CONTAINER_S3_PORT}"]
        if browser_port:
            ports.append(f"-p {int(browser_port)}:8081")
        if metrics_port:
            ports.append(f"-p {int(metrics_port)}:9080")

        # Match QE-validated deploy:
        # podman run -d --name rgw-standalone1 -p 7401:7480 \
        #   -v DATA:/var/lib/ceph/rgw_posix_driver:Z \
        #   -v POSIX:/tmp/rgw_posix_driver:Z IMAGE
        run_cmd = (
            f"podman run -d --name {name} "
            f"{' '.join(ports)} "
            f"-v {data_dir}:/var/lib/ceph/rgw_posix_driver:Z "
            f"-v {posix_dir}:/tmp/rgw_posix_driver:Z "
            f"{image}"
        )
        log.info(f"Running: {run_cmd}")
        node.exec_command(sudo=True, cmd=run_cmd, long_running=True)

    # Resolve container id
    cid_out, _ = node.exec_command(
        sudo=True,
        cmd=f"podman inspect -f '{{{{.Id}}}}' {name}",
    )
    container_id = (cid_out or "").strip()
    if not container_id:
        log.error("Failed to resolve container id after deploy")
        return 1

    endpoint = f"http://127.0.0.1:{host_port}"
    if not _wait_for_endpoint(node, endpoint):
        logs, _ = node.exec_command(
            sudo=True, cmd=f"podman logs --tail 80 {name}", check_ec=False
        )
        log.error(f"Zipper endpoint did not become ready. logs:\n{logs}")
        return 1

    # Probe admin CLI
    admin_out, _ = node.exec_command(
        sudo=True,
        cmd=f"podman exec {name} which rgw-standalone-admin || "
        f"podman exec {name} which radosgw-admin",
        check_ec=False,
    )
    admin_bin = "rgw-standalone-admin"
    if "rgw-standalone-admin" not in (admin_out or ""):
        admin_bin = "radosgw-admin"
        log.warning("rgw-standalone-admin not found; falling back to radosgw-admin")

    meta = {
        "endpoint": endpoint,
        "host_port": host_port,
        "container_name": name,
        "container_id": container_id,
        "image": image,
        "admin_bin": admin_bin,
        "data_dir": data_dir,
        "posix_dir": posix_dir,
        "browser_port": browser_port,
        "metrics_port": metrics_port,
    }
    remote = node.remote_file(file_name=ENDPOINT_META, file_mode="w", sudo=True)
    remote.write(json.dumps(meta, indent=2))
    remote.flush()
    log.info(f"Wrote zipper endpoint metadata: {meta}")
    return 0
