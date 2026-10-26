"""
Deploy IBM Ceph RGW Standalone / developer-experience (Zipper) via podman.

Validated QE command (ceph-hsm-92-yg8s5b-node6)::

    mkdir /root/my_zgw_data_dev_exp
    podman run -d --name dev-edition1 \\
      -p 7411:7480 -p 8091:8081 -p 9091:9080 \\
      -e ENDPOINT="http://<node-ip>:9091" \\
      -v /root/my_zgw_data_dev_exp:/var/lib/ceph/rgw_posix_driver:Z \\
      -v /root/my_zgw_data_dev_exp:/tmp/rgw_posix_driver:Z \\
      preprod.icr.io/cp/ibm-ceph/rgw-developer-experience-rhel10:v9.9.2-26951

Port map (developer-experience image)::

    zgw-host-port      → container 7480  (direct Zipper/RGW S3)
    browser-port       → container 8081  (S3 browser UI)
    s3browser-s3-port  → container 9080  (S3 API via s3browser; same role as 7480)

Registry login uses the same helper path as other IBM image pulls
(``get_registry_details`` + ``Registry.login``):
  - preprod.icr.io  → credentials.registry.ibm.preprod
  - cp.icr.io       → credentials.registry.ibm.cdn (via legacy/flat fallback)
  - cp.stg.icr.io / preprod.stg.icr.io → credentials.registry.ibm.stage

Writes endpoint metadata to /tmp/rgw_standalone_endpoint.json for
sanity_rgw.py (use-standalone: true).
"""

import json
import time

from cli.utilities.configs import get_registry_details
from cli.utilities.containers import Registry
from utility.log import Log

log = Log(__name__)

ENDPOINT_META = "/tmp/rgw_standalone_endpoint.json"

# Defaults match the validated developer-experience QE deploy
DEFAULT_IMAGE = (
    "preprod.icr.io/cp/ibm-ceph/rgw-developer-experience-rhel10:v9.9.2-26951"
)
DEFAULT_NAME = "dev-edition1"
DEFAULT_ZGW_HOST_PORT = 7411
DEFAULT_BROWSER_PORT = 8091
# S3 API exposed by s3browser (container 9080); not a metrics/prometheus port
DEFAULT_S3BROWSER_S3_PORT = 9091
CONTAINER_S3_PORT = 7480
CONTAINER_BROWSER_PORT = 8081
CONTAINER_S3BROWSER_S3_PORT = 9080
DATA_DIR = "/root/my_zgw_data_dev_exp"


def _registry_login(node, image, ibm_build=True):
    """podman login using the same credential resolution as other IBM pulls."""
    details = get_registry_details(ibm_build=ibm_build, image=image)
    registry = details["registry-url"]
    username = details["registry-username"]
    password = details["registry-password"]
    if not registry or not username or not password:
        log.warning(
            "Incomplete registry credentials for %s; assuming image is cached",
            image,
        )
        return

    log.info(f"podman login to {registry} (image host from {image})")
    Registry(node).login(registry, username, password)


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
    # Check if passed dynamically via custom_config from Jenkins/manifest
    image = (
        config.get("rgw_developer_experience_image")
        or config.get("image")
        or DEFAULT_IMAGE
    )
    name = config.get("container-name", DEFAULT_NAME)
    # Direct Zipper/RGW S3 host publish of container 7480.
    # Legacy key "host-port" is still accepted.
    zgw_host_port = int(
        config.get(
            "zgw-host-port",
            config.get("host-port", DEFAULT_ZGW_HOST_PORT),
        )
    )
    # Developer-experience image publishes browser UI + s3browser S3 API.
    # Set browser-port / s3browser-s3-port to null/false to omit
    # (plain rgw-standalone). Legacy key "metrics-port" is still accepted.
    browser_port = config.get("browser-port", DEFAULT_BROWSER_PORT)
    s3browser_s3_port = config.get(
        "s3browser-s3-port",
        config.get("metrics-port", DEFAULT_S3BROWSER_S3_PORT),
    )
    if browser_port in (False, "false", "null", "None"):
        browser_port = None
    if s3browser_s3_port in (False, "false", "null", "None"):
        s3browser_s3_port = None
    if browser_port is not None:
        browser_port = int(browser_port)
    if s3browser_s3_port is not None:
        s3browser_s3_port = int(s3browser_s3_port)

    # Same host path bind-mounted to both container paths (QE pattern)
    data_dir = config.get("data-dir", DATA_DIR)
    posix_dir = config.get("posix-dir", data_dir)
    force_recreate = config.get("force-recreate", True)
    ibm_build = config.get("ibm_build", True)
    # Optional override for -e ENDPOINT=...; default uses node IP + s3browser S3
    endpoint_env = config.get("endpoint-env")

    target = (
        ceph_cluster.get_ceph_object("client")
        or ceph_cluster.get_ceph_object("rgw")
        or ceph_cluster.get_ceph_object("installer")
    )
    if not target:
        log.error("No client/rgw/installer node available for zipper deploy")
        return 1
    node = target.node

    log.info(f"Deploying RGW Standalone / Zipper on {node.hostname}: {image}")
    node.exec_command(
        sudo=True, cmd="which podman || yum install -y podman", check_ec=False
    )

    try:
        _registry_login(node, image, ibm_build=ibm_build)
    except Exception as exc:
        log.error(f"Registry login failed for {image}: {exc}")
        return 1

    node.exec_command(sudo=True, cmd=f"mkdir -p {data_dir} {posix_dir}")

    if force_recreate:
        node.exec_command(
            sudo=True,
            cmd=f"podman rm -f {name} 2>/dev/null || true",
            check_ec=False,
        )

    out, _ = node.exec_command(
        sudo=True,
        cmd=f"podman ps --filter name=^{name}$ --format '{{{{.Names}}}}'",
        check_ec=False,
    )
    if (out or "").strip() == name:
        log.info(f"Container {name} already running; reusing")
    else:
        ports = [f"-p {zgw_host_port}:{CONTAINER_S3_PORT}"]
        if browser_port is not None:
            ports.append(f"-p {browser_port}:{CONTAINER_BROWSER_PORT}")
        if s3browser_s3_port is not None:
            ports.append(f"-p {s3browser_s3_port}:{CONTAINER_S3BROWSER_S3_PORT}")

        env_args = []
        if s3browser_s3_port is not None:
            # Always bind ENDPOINT to this node's IP (QE: http://10.0.65.11:9091)
            node_ip = (
                getattr(node, "ip_address", None)
                or getattr(node, "private_ip", None)
                or str(node)
            )
            if not endpoint_env:
                endpoint_env = f"http://{node_ip}:{s3browser_s3_port}"
            env_args.append(f'-e ENDPOINT="{endpoint_env}"')
            log.info(f"Using -e ENDPOINT={endpoint_env} (node IP={node_ip})")

        # Match validated QE deploy:
        # podman run -d --name dev-edition1 \
        #   -p 7411:7480 -p 8091:8081 -p 9091:9080 \
        #   -e ENDPOINT="http://<node-ip>:9091" \
        #   -v DATA:/var/lib/ceph/rgw_posix_driver:Z \
        #   -v DATA:/tmp/rgw_posix_driver:Z IMAGE
        run_cmd = (
            f"podman run -d --name {name} "
            f"{' '.join(ports)} "
            f"{' '.join(env_args)} "
            f"-v {data_dir}:/var/lib/ceph/rgw_posix_driver:Z "
            f"-v {posix_dir}:/tmp/rgw_posix_driver:Z "
            f"{image}"
        )
        log.info(f"Running: {run_cmd}")
        node.exec_command(sudo=True, cmd=run_cmd, long_running=True)

    cid_out, _ = node.exec_command(
        sudo=True,
        cmd=f"podman inspect -f '{{{{.Id}}}}' {name}",
    )
    container_id = (cid_out or "").strip()
    if not container_id:
        log.error("Failed to resolve container id after deploy")
        return 1

    # Tests exec on the same node → localhost Zipper S3 port
    endpoint = f"http://127.0.0.1:{zgw_host_port}"
    if not _wait_for_endpoint(node, endpoint):
        logs, _ = node.exec_command(
            sudo=True, cmd=f"podman logs --tail 80 {name}", check_ec=False
        )
        log.error(f"Zipper endpoint did not become ready. logs:\n{logs}")
        return 1

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
        "zgw_host_port": zgw_host_port,
        "host_port": zgw_host_port,  # legacy alias for older consumers
        "container_name": name,
        "container_id": container_id,
        "image": image,
        "admin_bin": admin_bin,
        "data_dir": data_dir,
        "posix_dir": posix_dir,
        "browser_port": browser_port,
        "s3browser_s3_port": s3browser_s3_port,
        "endpoint_env": endpoint_env,
    }
    remote = node.remote_file(file_name=ENDPOINT_META, file_mode="w", sudo=True)
    remote.write(json.dumps(meta, indent=2))
    remote.flush()
    log.info(f"Wrote zipper endpoint metadata: {meta}")
    return 0
