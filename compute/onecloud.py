"""OneCloud provider implementation for CephVMNode."""

import ipaddress
import os
import re
import time
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import yaml

from utility.log import Log

from .exceptions import NodeDeleteFailure, NodeError

LOG = Log(__name__)

VM_READY_STATES = ("on", "running")
VM_POLL_INTERVAL = 30
VM_POLL_TIMEOUT = 1800  # 30 minutes
CLEANUP_VERIFY_INTERVAL = 5
CLEANUP_VERIFY_TIMEOUT = 900  # 15 minutes max to wait for deletion to complete

# VM name fields the API may return (OpenAPI uses vmname; some implementations use camelCase)
VM_NAME_KEYS = ("vmname", "vmName", "VMName", "name")

# Map inventory OS names to OneCloud API expected values (GET /vm/images?os=)
# API accepts: AIX, CentOS, RedHat, SuSE, Ubuntu, Windows
OS_HINT_TO_API = {
    "rhel": "RedHat",
    "centos": "CentOS",
    "windows": "Windows",
    "ubuntu": "Ubuntu",
    "sles": "SuSE",
    "suse": "SuSE",
    "aix": "AIX",
}


def parse_vm_list_from_response(vm_data: Any) -> List[Dict]:
    """
    Extract VM list from GET /vm or similar response.
    Handles multiple response shapes: data, data.virtualMachines, virtualMachines, or direct array.
    """
    if vm_data is None:
        return []
    if isinstance(vm_data, list):
        return [v for v in vm_data if isinstance(v, dict)]
    if not isinstance(vm_data, dict):
        return []
    # Try common wrapper keys
    for key in ("data", "virtualMachines", "vms"):
        val = vm_data.get(key)
        if isinstance(val, list):
            return [v for v in val if isinstance(v, dict)]
        if isinstance(val, dict) and "virtualMachines" in val:
            lst = val.get("virtualMachines")
            if isinstance(lst, list):
                return [v for v in lst if isinstance(v, dict)]
    return []


def get_vm_name(vm: Dict) -> Optional[str]:
    """Get VM name from various API field names. Returns None if not found."""
    if not isinstance(vm, dict):
        return None
    for key in VM_NAME_KEYS:
        name = vm.get(key)
        if name and isinstance(name, str):
            return name.strip() or None
    net = vm.get("network")
    if isinstance(net, dict):
        for key in ("hostname", "fqdn"):
            val = net.get(key)
            if val and isinstance(val, str):
                return val.split(".")[0].strip() or None
    return None


def get_vm_ip(vm: Dict) -> Optional[str]:
    """Get VM IP from various API field names. Returns None if not found."""
    if not isinstance(vm, dict):
        return None
    net = vm.get("network") or {}
    for key in ("ipaddr", "ipAddr", "ip_address", "public_ip", "floating_ip"):
        val = net.get(key) or vm.get(key)
        if val and isinstance(val, str) and val.strip():
            return val.strip()
    return None


def get_onecloud_client(
    api_key: str,
    base_url: str,
    verify_ssl: bool = False,
):
    """
    Return a simple requests-based client for OneCloud API.

    Args:
        api_key: JWT Bearer token for authentication.
        base_url: API base URL from credentials (global_credentials / osp-cred).
        verify_ssl: If False (default), disable SSL verification.

    Returns:
        Object with get/post/put/delete methods that add auth headers.
    """
    if not base_url:
        raise NodeError(
            "OneCloud: 'base_url' is required in credentials. "
            "Set it in osp-cred (onecloud-credentials) or cephci.yaml."
        )
    base = base_url.rstrip("/")

    def _request(method: str, path: str, **kwargs) -> requests.Response:
        url = f"{base}{path}" if path.startswith("/") else f"{base}/{path}"
        headers = kwargs.pop("headers", {})
        headers.setdefault("Accept", "application/json")
        headers.setdefault("Content-Type", "application/json")
        headers.setdefault("Authorization", f"Bearer {api_key}")
        kwargs.setdefault("verify", verify_ssl)
        return requests.request(method, url, headers=headers, timeout=120, **kwargs)

    class Client:
        def get(self, path: str, **kwargs) -> requests.Response:
            return _request("GET", path, **kwargs)

        def post(self, path: str, **kwargs) -> requests.Response:
            return _request("POST", path, **kwargs)

        def put(self, path: str, **kwargs) -> requests.Response:
            return _request("PUT", path, **kwargs)

        def delete(self, path: str, **kwargs) -> requests.Response:
            return _request("DELETE", path, **kwargs)

    return Client()


def process_onecloud_custom_config(custom_config: Optional[List[str]] = None) -> Dict:
    """
    Process custom config for OneCloud target site/project.

    Users can provide overrides via --custom-config onecloud_site=POK, etc.

    Args:
        custom_config: List of key=value (e.g. onecloud_site=POK).

    Returns:
        Dict with site, project_id, group_id, vlan, image_id, resources, arch.
    """
    repo_dir = Path(__file__).resolve().parent.parent
    overrides = {}

    if custom_config:
        overrides = dict(
            item.split("=", 1)
            for item in custom_config
            if "=" in item and item.split("=", 1)[0].startswith("onecloud")
        )

    # Platform config: use inventory/cred/--custom-config only (no default.yaml).
    # Optional: --custom-config onecloud_platform=X loads conf/onecloud/X.yaml if it exists.
    platform_name = overrides.get("onecloud_platform", "default")
    platform_dict = {}
    if platform_name != "default":
        platform_conf = repo_dir.joinpath(f"conf/onecloud/{platform_name}.yaml")
        if platform_conf.exists():
            with platform_conf.open() as fh:
                platform_dict = yaml.safe_load(fh) or {}

    # Override with custom_config (validate int values to avoid crashes)
    def _parse_int(key: str, override_key: str) -> None:
        try:
            platform_dict[key] = int(overrides[override_key])
        except (ValueError, TypeError):
            raise NodeError(f"OneCloud: {override_key} must be a valid integer")

    if "onecloud_site" in overrides:
        platform_dict["site"] = overrides["onecloud_site"]
    if "onecloud_project_id" in overrides:
        _parse_int("project_id", "onecloud_project_id")
    if "onecloud_group_id" in overrides:
        _parse_int("group_id", "onecloud_group_id")
    if "onecloud_vlan" in overrides:
        _parse_int("vlan", "onecloud_vlan")
    if "onecloud_image_id" in overrides:
        _parse_int("image_id", "onecloud_image_id")
    if "onecloud_resources" in overrides:
        platform_dict["resources"] = overrides["onecloud_resources"]

    return platform_dict


def expand_private_key_path(path: Optional[str]) -> str:
    """Expand ~ in path; return empty string if path is empty or invalid."""
    if not path or not str(path).strip():
        return ""
    return os.path.expanduser(str(path).strip())


def generate_onecloud_node_name(
    run_id: str, node_key: str, role: Any, max_length: int = 25
) -> str:
    """
    Return VM name for OneCloud API (max 25 chars, alphanumeric + hyphens only).

    Args:
        run_id: Unique run ID (e.g. 20DBDH).
        node_key: Node key (e.g. node1, node2).
        role: RolesContainer with node roles (unused, kept for API compatibility).
        max_length: Max VM name length (OneCloud default 25).

    Returns:
        Name like ci-20DBDH-node1, ci-20DBDH-node2.
    """
    node_num = "".join(c for c in node_key if c.isdigit()) or "0"
    name = f"ci-{run_id}-node{node_num}"
    if len(name) > max_length:
        name = name[:max_length]
    return name


def _parse_images_response(data: Any) -> List[Dict]:
    """Extract image list from GET /vm/images response."""
    if data is None:
        return []
    if isinstance(data, list):
        return [i for i in data if isinstance(i, dict)]
    if not isinstance(data, dict):
        return []
    for key in ("data", "images"):
        val = data.get(key)
        if isinstance(val, list):
            return [i for i in val if isinstance(i, dict)]
        if isinstance(val, dict) and "images" in val:
            lst = val.get("images")
            if isinstance(lst, list):
                return [i for i in lst if isinstance(i, dict)]
    return []


def _image_display_name(img: Dict) -> str:
    """Return display name / os_release / name for an image dict."""
    return str(
        img.get("display_name") or img.get("os_release") or img.get("name") or ""
    )


def _image_id(img: Dict) -> Optional[int]:
    """Extract image ID from image dict."""
    v = img.get("imageid") or img.get("id") or img.get("template_id")
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _image_matches_site(img: Dict, site: str) -> bool:
    """Return True if image is available at the given site."""
    site_upper = (site or "").strip().upper()
    if not site_upper:
        return True
    for key in ("site", "site_id", "sites", "site_ids", "site_name"):
        val = img.get(key)
        if val is None:
            continue
        if isinstance(val, str):
            if val.strip().upper() == site_upper:
                return True
        elif isinstance(val, (int, float)):
            # site_id might be numeric; we can't map without sites API
            return True  # Assume match if present
        elif isinstance(val, list):
            if any(
                (str(v).strip().upper() == site_upper if isinstance(v, str) else True)
                for v in val
            ):
                return True
    return False


def _image_is_windows(img: Dict) -> bool:
    """Return True if image appears to be Windows (exclude - RHEL/Ceph requires Linux)."""
    name = _image_display_name(img).lower()
    os_type = str(
        img.get("os") or img.get("os_type") or img.get("operating_system") or ""
    ).lower()
    for term in ("windows", "microsoft"):
        if term in name or term in os_type:
            return True
    return False


def _image_matches_arch(img: Dict, arch: str) -> bool:
    """Return True if image matches the requested architecture (e.g. x86_64)."""
    if not arch:
        return True
    arch_lower = arch.strip().lower()
    img_arch = (
        str(img.get("arch") or img.get("architecture") or img.get("arch_id") or "")
    ).lower()
    if img_arch and arch_lower in img_arch:
        return True
    # x86_64 aliases
    if arch_lower in ("x86_64", "amd64", "x86-64"):
        return not img_arch or img_arch in ("x86_64", "amd64", "x86-64")
    return not img_arch or img_arch == arch_lower


def _image_matches_platform(img: Dict, platform_filter: str) -> bool:
    """Return True if image name/os_release matches the platform (e.g. rhel-9, rhel-10)."""
    if not platform_filter or not isinstance(platform_filter, str):
        return True
    pf = platform_filter.strip().lower()
    if not pf.startswith("rhel-"):
        return True
    # Extract major version: rhel-9 -> 9, rhel-10 -> 10
    m = re.match(r"rhel-(\d+)(?:\.\d+)?", pf)
    if not m:
        return True
    major = m.group(1)
    # Check version_id if present (e.g. 9.7, 10.1)
    ver = img.get("version_id") or img.get("version") or img.get("os_version")
    if ver is not None and str(ver).strip():
        ver_str = str(ver).strip()
        if ver_str.startswith(major + ".") or ver_str == major:
            return True
    name = _image_display_name(img).lower()
    # Match rhel-9, rhel-9.7, RHEL 9, 9.7, etc. Avoid rhel-19 matching rhel-9
    pattern = rf"(?:^|[^0-9])rhel-{major}(?:\D|$)|rhel\s+{major}(?:\D|$)|(?:^|[^0-9]){major}\.\d+"
    return bool(re.search(pattern, name))


def _image_matches_version(img: Dict, version_preference: str) -> bool:
    """Return True if image version matches the preference (e.g. 9.7, 10.1)."""
    if not version_preference or not isinstance(version_preference, str):
        return False
    vp = str(version_preference).strip()
    if not vp:
        return False
    ver = img.get("version_id") or img.get("version") or img.get("os_version")
    if ver is not None and str(ver).strip() == vp:
        return True
    name = _image_display_name(img).lower()
    # Match "9.7", "rhel 9.7", "rhel-9.7", etc.
    return vp in name or f"rhel-{vp}" in name or f"rhel {vp}" in name


def _image_version_number(img: Dict) -> Optional[float]:
    """Extract numeric version (e.g. 9.7) from image for sorting."""
    ver = img.get("version_id") or img.get("version") or img.get("os_version")
    if ver is not None:
        try:
            return float(str(ver).strip())
        except (ValueError, TypeError):
            pass
    name = _image_display_name(img)
    m = re.search(r"(\d+\.\d+)", name)
    if m:
        try:
            return float(m.group(1))
        except (ValueError, TypeError):
            pass
    return None


def _sort_images_by_version_proximity(
    images: List[Dict], target_version: str
) -> List[Dict]:
    """Sort images by proximity to target version (closest first)."""
    try:
        target = float(target_version)
    except (ValueError, TypeError):
        return images
    scored = []
    for img in images:
        ver = _image_version_number(img)
        distance = abs(ver - target) if ver is not None else 999.0
        scored.append((distance, img))
    scored.sort(key=lambda x: x[0])
    return [img for _, img in scored]


def resolve_image_for_site(
    client,
    site: str,
    preferred_image_id: Optional[int] = None,
    arch: Optional[str] = None,
    os_hint: Optional[str] = None,
    exclude_image_ids: Optional[List[int]] = None,
    platform_filter: Optional[str] = None,
    version_preference: Optional[str] = None,
) -> int:
    """
    Resolve an image ID that is available at the given site.

    Tries GET /vm/images?site=X first; falls back to GET /vm/images and filters
    by site if images have site info. Uses preferred_image_id if valid at site,
    otherwise picks the first available image. When platform_filter is set
    (e.g. rhel-9, rhel-10), only images matching that RHEL version are considered.
    When version_preference is set (e.g. 9.7 from inventory version_id), images
    matching that exact version are preferred over others (e.g. 9.7 over 9.4).

    Args:
        client: OneCloud API client.
        site: Site code (e.g. TUC, POK).
        preferred_image_id: User's preferred image; used if available at site.
        arch: Optional arch filter (x86_64, s390x, ppc64le).
        os_hint: Optional OS filter (RHEL/RedHat, CentOS, etc.). RHEL maps to RedHat for API.
        exclude_image_ids: Image IDs to exclude (e.g. after deploy rejected one).
        platform_filter: RHEL platform to match (e.g. rhel-9, rhel-10).
        version_preference: Exact version to prefer (e.g. 9.7 from inventory version_id).

    Returns:
        Resolved image ID (int).

    Raises:
        NodeError: If no images found for the site.
    """
    site_upper = (site or "").strip().upper()
    params = {}
    if site_upper:
        params["site"] = site_upper
    if arch:
        params["arch"] = arch
    if os_hint:
        # Map RHEL/RedHat variants to API-expected value (RedHat)
        hint = str(os_hint).strip().lower()
        api_os = OS_HINT_TO_API.get(hint, str(os_hint).strip())
        params["os"] = api_os

    # Try site-filtered request first
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        resp = client.get(f"/vm/images?{qs}")
    else:
        resp = client.get("/vm/images")

    if resp.status_code != 200:
        resp = client.get("/vm/images")
    if resp.status_code != 200:
        if preferred_image_id is not None:
            LOG.warning(
                "OneCloud: GET /vm/images failed (%s), using preferred image_id %s",
                resp.status_code,
                preferred_image_id,
            )
            return int(preferred_image_id)
        raise NodeError(
            f"OneCloud: cannot list images ({resp.status_code}). "
            "Set image_id in osp-cred or inventory."
        )

    data = resp.json()
    images = _parse_images_response(data)
    if not images:
        if preferred_image_id is not None:
            LOG.warning(
                "OneCloud: no images in response, using preferred image_id %s",
                preferred_image_id,
            )
            return int(preferred_image_id)
        raise NodeError(
            "OneCloud: no images available. Set image_id in osp-cred or inventory."
        )

    # Exclude Windows - RHEL/Ceph requires Linux; platform filter may not catch all
    for_site = [i for i in images if not _image_is_windows(i)]
    if len(for_site) < len(images):
        LOG.info(
            "OneCloud: excluded %d Windows image(s), %d RHEL/Linux remaining",
            len(images) - len(for_site),
            len(for_site),
        )
    if not for_site:
        raise NodeError(
            f"OneCloud: no non-Windows images at site {site_upper}. "
            "Ensure RHEL images are available."
        )

    # Filter by architecture (x86_64, etc.) when specified
    if arch:
        for_arch = [i for i in for_site if _image_matches_arch(i, arch)]
        if for_arch:
            for_site = for_arch
            LOG.info(
                "OneCloud: filtering images by arch %s (%d match)",
                arch,
                len(for_site),
            )
        else:
            LOG.warning(
                "OneCloud: no images match arch %s, using all (API may not provide arch metadata)",
                arch,
            )

    # Filter by site if we have site info in images
    for_site_filtered = [i for i in for_site if _image_matches_site(i, site_upper)]
    if for_site_filtered:
        for_site = for_site_filtered
    # else: keep for_site as-is (no site info in images; API may have pre-filtered)

    # Filter by platform (rhel-9, rhel-10) when --platform is passed
    if platform_filter:
        for_platform = [
            i for i in for_site if _image_matches_platform(i, platform_filter)
        ]
        if for_platform:
            for_site = for_platform
            LOG.info(
                "OneCloud: filtering images by platform %s (%d match)",
                platform_filter,
                len(for_site),
            )
        else:
            raise NodeError(
                f"OneCloud: no images match platform {platform_filter!r} at site {site_upper}. "
                "Verify image metadata (display_name, os_release) or set image_id in osp-cred/inventory."
            )

    exclude = set(exclude_image_ids or [])

    def _pick_first_valid(imgs: List[Dict]) -> Optional[tuple]:
        """Return (image_id, img) for first valid image, or None."""
        for img in imgs:
            iid = _image_id(img)
            if iid is not None and iid not in exclude:
                return (iid, img)
        return None

    # Prefer user's choice if it's in the available (and platform-filtered) list and not excluded
    if preferred_image_id is not None and preferred_image_id not in exclude:
        pid = int(preferred_image_id)
        for img in for_site:
            if _image_id(img) == pid:
                LOG.info(
                    "OneCloud: using image_id %s (preferred, available at site %s)",
                    pid,
                    site_upper or "?",
                )
                return pid
        LOG.info(
            "OneCloud: image_id %s not available at site %s (or does not match platform); selecting from site images",
            preferred_image_id,
            site_upper,
        )

    # Prefer images matching version_preference (e.g. 9.7 from inventory) when set
    for_site_before_version = (
        for_site  # Keep for fallback if all version matches are excluded
    )
    available_versions = [
        (
            _image_id(i),
            _image_version_number(i),
            _image_display_name(i)[:40],
        )
        for i in for_site
    ]
    LOG.debug(
        "OneCloud: available images after filtering: %s",
        [(vid, ver, n) for vid, ver, n in available_versions],
    )
    if version_preference:
        matching = [
            i for i in for_site if _image_matches_version(i, version_preference)
        ]
        if matching:
            for_site = matching
            LOG.info(
                "OneCloud: preferring images matching version %s (%d match)",
                version_preference,
                len(for_site),
            )

    # Pick first valid image not in exclude list
    picked = _pick_first_valid(for_site)
    if picked is not None:
        iid, img = picked
        name = _image_display_name(img)
        LOG.info(
            "OneCloud: using image_id %s at site %s (%s)",
            iid,
            site_upper or "?",
            name[:50] if name else "?",
        )
        return iid

    # All version-preferred images excluded? Fall back to closest available version
    if (
        version_preference
        and for_site_before_version
        and for_site != for_site_before_version
    ):
        sorted_by_proximity = _sort_images_by_version_proximity(
            for_site_before_version, version_preference
        )
        fallback_options = [
            (_image_id(i), _image_version_number(i), _image_display_name(i)[:40])
            for i in sorted_by_proximity
        ]
        LOG.debug(
            "OneCloud: version %s unavailable, fallback options (closest first): %s",
            version_preference,
            fallback_options,
        )
        picked = _pick_first_valid(sorted_by_proximity)
        if picked is not None:
            iid, img = picked
            name = _image_display_name(img)
            ver = _image_version_number(img)
            LOG.warning(
                "OneCloud: version %s not available; using closest: image_id %s "
                "(%s, version %s)",
                version_preference,
                iid,
                name[:50] if name else "?",
                ver or "?",
            )
            return iid

    if preferred_image_id is not None and preferred_image_id not in exclude:
        # Validate preferred is not Windows before using as last resort
        for img in images:
            if _image_id(img) == preferred_image_id:
                if _image_is_windows(img):
                    raise NodeError(
                        f"OneCloud: preferred image_id {preferred_image_id} is Windows. "
                        "Use a RHEL image_id in osp-cred or inventory."
                    )
                break
        return int(preferred_image_id)
    raise NodeError(
        f"OneCloud: no valid image for site {site_upper}. "
        "Set image_id in osp-cred or inventory."
    )


def _parse_projects_response(data: Any) -> List[Dict]:
    """Extract project list from GET /projects response."""
    if data is None:
        return []
    if isinstance(data, list):
        return [p for p in data if isinstance(p, dict)]
    if isinstance(data, dict):
        val = data.get("data", data)
        return val if isinstance(val, list) else []
    return []


def _project_matches_site(proj: Dict, site: str) -> bool:
    """Return True if project is available at the given site."""
    site_upper = (site or "").strip().upper()
    if not site_upper:
        return True
    for key in ("site", "site_id", "sites", "site_ids"):
        val = proj.get(key)
        if val is None:
            continue
        if isinstance(val, str):
            if val.strip().upper() == site_upper:
                return True
        elif isinstance(val, list):
            if any(
                str(v).strip().upper() == site_upper for v in val if isinstance(v, str)
            ):
                return True
    return True  # If no site info, assume match


def resolve_project_for_site(
    client,
    site: str,
    preferred_project_id: Optional[int] = None,
) -> int:
    """
    Resolve a project ID for the given site.

    Tries GET /projects?site=X first; falls back to GET /projects.
    Uses preferred_project_id if valid, otherwise picks first available project.

    Args:
        client: OneCloud API client.
        site: Site code (e.g. TUC, POK).
        preferred_project_id: User's preferred project; used if available.

    Returns:
        Resolved project ID (int).

    Raises:
        NodeError: If no projects found.
    """
    site_upper = (site or "").strip().upper()
    resp = (
        client.get(f"/projects?site={site_upper}")
        if site_upper
        else client.get("/projects")
    )
    if resp.status_code != 200:
        resp = client.get("/projects")
    if resp.status_code != 200:
        if preferred_project_id is not None:
            LOG.warning(
                "OneCloud: GET /projects failed (%s), using preferred project_id %s",
                resp.status_code,
                preferred_project_id,
            )
            return int(preferred_project_id)
        raise NodeError(
            "OneCloud: cannot list projects. Set project_id in osp-cred or inventory."
        )

    data = resp.json()
    projects = _parse_projects_response(data)
    if not projects:
        if preferred_project_id is not None:
            return int(preferred_project_id)
        raise NodeError(
            "OneCloud: no projects available. Set project_id in osp-cred or inventory."
        )

    for_site = [p for p in projects if _project_matches_site(p, site_upper)]
    if not for_site:
        for_site = projects

    def _proj_id(p: Dict) -> Optional[int]:
        v = p.get("projectid") or p.get("id")
        try:
            return int(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    if preferred_project_id is not None:
        pid = int(preferred_project_id)
        for p in for_site:
            if _proj_id(p) == pid:
                return pid
        LOG.info(
            "OneCloud: project_id %s not in site list; selecting from available",
            preferred_project_id,
        )

    for p in for_site:
        pid = _proj_id(p)
        if pid is not None:
            name = p.get("projectname") or p.get("name") or ""
            LOG.info(
                "OneCloud: using project_id %s at site %s (%s)",
                pid,
                site_upper or "?",
                name[:40] if name else "?",
            )
            return pid

    if preferred_project_id is not None:
        return int(preferred_project_id)
    raise NodeError(
        "OneCloud: no valid project for site. Set project_id in osp-cred or inventory."
    )


def get_vlan_for_site(
    client,
    site: str,
    preferred_vlan: Optional[int] = None,
) -> Optional[int]:
    """
    Resolve a valid VLAN for the given site using GET /networks.

    If preferred_vlan is valid for the site, returns it. Otherwise picks the first
    available VLAN for that site. If the API returns no VLAN IDs (e.g. "Default Pool"
    with vlan: null), returns None to omit VLAN—the API may then use the Default network.

    Args:
        client: OneCloud API client (from get_onecloud_client).
        site: Site code (e.g. TUC, POK).
        preferred_vlan: VLAN from config; used if valid or as fallback.

    Returns:
        VLAN ID to use for deploy, or None to omit (API may use "Default" network).
    """
    fallback = int(preferred_vlan) if preferred_vlan is not None else None
    site_upper = (site or "").strip().upper()
    if not site_upper:
        return fallback

    try:
        # Try site-specific endpoint first (some APIs support ?site=)
        resp = client.get(f"/networks?site={site_upper}")
        if resp.status_code != 200:
            resp = client.get("/networks")
        if resp.status_code != 200:
            LOG.warning(
                "OneCloud: GET /networks failed (%s), omitting VLAN for site %s (API may use Default)",
                resp.status_code,
                site_upper,
            )
            return None

        data = resp.json()
        networks = data.get("data", data) if isinstance(data, dict) else data
        if isinstance(networks, dict):
            # Some APIs return {site: [networks]} or {site: {vlans: [...]}}
            site_networks = networks.get(site_upper) or networks.get(site_upper.lower())
            if isinstance(site_networks, list):
                networks = site_networks
            elif isinstance(site_networks, dict):
                networks = site_networks.get("networks", site_networks.get("vlans", []))
            else:
                networks = []
        else:
            networks = networks if isinstance(networks, list) else []
    except Exception as e:
        LOG.warning(
            "OneCloud: could not fetch networks (%s), omitting VLAN for site %s (API may use Default)",
            e,
            site_upper,
        )
        return None

    SITE_ALIASES = {
        "TUC": ["TUC", "TUCSON"],
        "POK": ["POK", "POUGHKEEPSIE"],
        "AUS": ["AUS"],
        "RCH": ["RCH"],
    }

    def _site_from_net(net):
        s = (
            net.get("site")
            or net.get("site_name")
            or net.get("site_id")
            or net.get("identifier")
            or net.get("location")
        )
        if isinstance(s, dict):
            s = s.get("identifier") or s.get("name") or ""
        s = (str(s or "")).strip().upper()
        # Normalize full names to site codes
        for code, aliases in SITE_ALIASES.items():
            if any(s == a or s.startswith(a) for a in aliases):
                return code
        return s

    vlans_for_site = []
    for n in networks:
        net_site = _site_from_net(n)
        net_vlan = n.get("vlan") or n.get("VLAN") or n.get("vlan_id")
        if net_vlan is not None:
            try:
                net_vlan = int(net_vlan)
            except (TypeError, ValueError):
                continue
        else:
            continue
        if net_site == site_upper:
            vlans_for_site.append(net_vlan)

    vlans_for_site = sorted(set(vlans_for_site))
    if not vlans_for_site:
        # GET /networks returned no VLAN IDs (e.g. "Default Pool" with vlan: null).
        # Omit VLAN so API may use "Default" network like the portal does.
        LOG.info(
            "OneCloud: no VLAN IDs in GET /networks for site %s, omitting VLAN (API may use Default network)",
            site_upper,
        )
        return None

    if preferred_vlan is not None and int(preferred_vlan) in vlans_for_site:
        return int(preferred_vlan)

    chosen = vlans_for_site[0]
    if preferred_vlan is not None and int(preferred_vlan) != chosen:
        LOG.info(
            "OneCloud: VLAN %s not available at site %s; using VLAN %s (available: %s)",
            preferred_vlan,
            site_upper,
            chosen,
            vlans_for_site[:10],
        )
    return chosen


def cleanup_onecloud_ceph_nodes(
    onecloud_cred: Dict,
    pattern: str,
    custom_config: Optional[List[str]] = None,
) -> None:
    """
    Clean up OneCloud clusters and VMs matching the given pattern.

    GET /clusters, filter by cluster_name containing pattern, then GET /vm?clusterid=X,
    DELETE /vm/{id} for each VM, and DELETE /clusters/{id} for the cluster (if supported).

    Args:
        onecloud_cred: Credentials with globals["onecloud-credentials"].
        pattern: Pattern to match cluster name (e.g. run id or prefix).
        custom_config: Optional list of key=value for platform overrides.
    """
    glbs = onecloud_cred.get("globals") or {}
    cred = glbs.get("onecloud-credentials")
    if not cred:
        raise NodeError("Missing 'onecloud-credentials' in globals")

    # Handle None/empty pattern to avoid TypeError and accidental match-all
    pattern = (pattern or "").strip()
    if not pattern:
        try:
            pattern = f"-{os.getlogin()}-"
        except OSError:
            pattern = "-cephci-"

    api_key = cred.get("api_key")
    base_url = cred.get("base_url")
    verify_ssl = cred.get("verify_ssl", False)
    if not api_key:
        raise NodeError("Missing 'api_key' in onecloud-credentials")
    if not base_url:
        raise NodeError(
            "Missing 'base_url' in onecloud-credentials. "
            "Set it in osp-cred or cephci.yaml."
        )

    client = get_onecloud_client(api_key, base_url, verify_ssl=verify_ssl)

    LOG.info("Listing OneCloud clusters for cleanup (pattern=%s)", pattern)
    resp = client.get("/clusters")
    if resp.status_code != 200:
        LOG.warning("Failed to list clusters: %s %s", resp.status_code, resp.text[:200])
        return

    data = resp.json()
    clusters = data.get("data", []) if isinstance(data, dict) else data
    if not isinstance(clusters, list):
        clusters = []

    matching = [c for c in clusters if pattern in c.get("cluster_name", "")]
    if not matching:
        LOG.info("No clusters matching pattern '%s'", pattern)
        return

    LOG.info("Cleaning up %d clusters matching pattern", len(matching))

    for cluster in matching:
        cluster_id = cluster.get("clusterid") or cluster.get("clusterID")
        cluster_name = cluster.get("cluster_name", "?")
        if not cluster_id:
            continue

        # List VMs in cluster
        vm_resp = client.get(f"/vm?clusterid={cluster_id}")
        if vm_resp.status_code != 200:
            LOG.warning(
                "Failed to list VMs for cluster %s: %s", cluster_id, vm_resp.status_code
            )
            continue

        vm_data = vm_resp.json()
        vms = vm_data.get("data", []) if isinstance(vm_data, dict) else vm_data
        if not isinstance(vms, list):
            vms = []

        for vm in vms:
            vmid = vm.get("vmid") or vm.get("vmID")
            if vmid is None:
                continue
            try:
                del_resp = client.delete(f"/vm/{vmid}")
                if del_resp.status_code in (200, 204):
                    LOG.info("Deleted VM %s (cluster %s)", vmid, cluster_name)
                else:
                    LOG.warning(
                        "Failed to delete VM %s: %s", vmid, del_resp.status_code
                    )
            except Exception as e:
                LOG.warning("Error deleting VM %s: %s", vmid, e)

        # Verify VMs are gone before proceeding (API may be eventually consistent)
        def _active_vms(vm_list: List[Dict]) -> List[Dict]:
            """Filter to VMs that appear active (not deleted/terminated)."""
            terminal = {"deleted", "terminated", "off"}
            return [
                v
                for v in vm_list
                if (v.get("state") or v.get("status") or "").lower() not in terminal
            ]

        vm_resp = client.get(f"/vm?clusterid={cluster_id}")
        if vm_resp.status_code == 200:
            try:
                vm_resp_alt = client.get(f"/vm?clusterID={cluster_id}")
                if vm_resp_alt.status_code == 200:
                    vm_resp = vm_resp_alt
            except Exception:
                pass
        all_vms = (
            parse_vm_list_from_response(vm_resp.json())
            if vm_resp.status_code == 200
            else []
        )
        remaining = _active_vms(all_vms)
        if remaining:
            LOG.info(
                "OneCloud: verifying deletion for cluster %s (%d VM(s) still listed), polling up to %ds",
                cluster_name,
                len(remaining),
                CLEANUP_VERIFY_TIMEOUT,
            )
            deadline = time.time() + CLEANUP_VERIFY_TIMEOUT
            while time.time() < deadline and remaining:
                time.sleep(CLEANUP_VERIFY_INTERVAL)
                vm_resp = client.get(f"/vm?clusterid={cluster_id}")
                if vm_resp.status_code != 200:
                    vm_resp = client.get(f"/vm?clusterID={cluster_id}")
                all_vms = (
                    parse_vm_list_from_response(vm_resp.json())
                    if vm_resp.status_code == 200
                    else []
                )
                remaining = _active_vms(all_vms)
                if remaining:
                    LOG.info(
                        "OneCloud: cluster %s still has %d VM(s), waiting...",
                        cluster_name,
                        len(remaining),
                    )
            if remaining:
                LOG.warning(
                    "OneCloud: cluster %s still reports %d VM(s) after %ds; create may use stale data",
                    cluster_name,
                    len(remaining),
                    CLEANUP_VERIFY_TIMEOUT,
                )
            else:
                LOG.info("OneCloud: cluster %s verified empty", cluster_name)

        # Delete cluster after VMs (API may support DELETE /clusters/{id})
        try:
            cluster_del_resp = client.delete(f"/clusters/{cluster_id}")
            if cluster_del_resp.status_code in (200, 204):
                LOG.info("Deleted cluster %s (%s)", cluster_id, cluster_name)
            elif cluster_del_resp.status_code in (404, 405, 501):
                LOG.info(
                    "Cluster delete not supported (API %s), cluster %s may remain",
                    cluster_del_resp.status_code,
                    cluster_name,
                )
            else:
                LOG.warning(
                    "Failed to delete cluster %s: %s %s",
                    cluster_id,
                    cluster_del_resp.status_code,
                    cluster_del_resp.text[:200],
                )
        except Exception as e:
            LOG.warning("Error deleting cluster %s: %s", cluster_id, e)

    if matching:
        time.sleep(5)  # allow backend to settle before create
    LOG.info("Done cleaning up OneCloud nodes with pattern %s", pattern)


def vm_start(client, vmid: int) -> None:
    """
    Start a VM.

    Args:
        client: OneCloud API client.
        vmid: VM ID.

    Raises:
        NodeError: If the API call fails.
    """
    for path in (f"/vm/{vmid}/start", f"/vm/{vmid}/power_on"):
        resp = client.post(path, json={})
        if resp.status_code in (200, 201, 202, 204):
            LOG.info("OneCloud: started VM %s", vmid)
            return
        if resp.status_code == 404:
            continue
        raise NodeError(
            f"OneCloud: failed to start VM {vmid}: {resp.status_code} {resp.text[:200]}"
        )
    raise NodeError(
        f"OneCloud: VM start not supported (404 for VM {vmid}). "
        "Verify API supports /vm/{{id}}/start or /vm/{{id}}/power_on."
    )


def vm_stop(client, vmid: int) -> None:
    """
    Stop a VM.

    Args:
        client: OneCloud API client.
        vmid: VM ID.

    Raises:
        NodeError: If the API call fails.
    """
    for path in (f"/vm/{vmid}/stop", f"/vm/{vmid}/power_off"):
        resp = client.post(path, json={})
        if resp.status_code in (200, 201, 202, 204):
            LOG.info("OneCloud: stopped VM %s", vmid)
            return
        if resp.status_code == 404:
            continue
        raise NodeError(
            f"OneCloud: failed to stop VM {vmid}: {resp.status_code} {resp.text[:200]}"
        )
    raise NodeError(
        f"OneCloud: VM stop not supported (404 for VM {vmid}). "
        "Verify API supports /vm/{{id}}/stop or /vm/{{id}}/power_off."
    )


def vm_restart(client, vmid: int) -> None:
    """
    Restart a VM.

    Args:
        client: OneCloud API client.
        vmid: VM ID.

    Raises:
        NodeError: If the API call fails.
    """
    for path in (f"/vm/{vmid}/restart", f"/vm/{vmid}/reboot"):
        resp = client.post(path, json={})
        if resp.status_code in (200, 201, 202, 204):
            LOG.info("OneCloud: restarted VM %s", vmid)
            return
        if resp.status_code == 404:
            continue
        raise NodeError(
            f"OneCloud: failed to restart VM {vmid}: {resp.status_code} {resp.text[:200]}"
        )
    raise NodeError(
        f"OneCloud: VM restart not supported (404 for VM {vmid}). "
        "Verify API supports /vm/{{id}}/restart or /vm/{{id}}/reboot."
    )


def _wait_until_vm_state(
    client,
    vmid: int,
    target_state: str,
    timeout: int = VM_POLL_TIMEOUT,
) -> None:
    """Poll VM until it reaches target_state (e.g. 'on', 'off', 'stopped')."""
    target_lower = target_state.lower()
    start = time.time()
    while time.time() - start < timeout:
        resp = client.get(f"/vm/{vmid}")
        if resp.status_code != 200:
            time.sleep(VM_POLL_INTERVAL)
            continue
        data = resp.json()
        vm = data.get("data", data) if isinstance(data, dict) else {}
        if isinstance(vm, dict):
            state = (vm.get("state") or vm.get("status") or "").lower()
            if state == target_lower:
                return
        time.sleep(VM_POLL_INTERVAL)
    raise NodeError(
        f"OneCloud: VM {vmid} did not reach state {target_state!r} within {timeout}s"
    )


def floating_ip_provision(
    client,
    cluster_id: int,
    site: str,
    vlan: int,
) -> Optional[str]:
    """
    Provision a floating IP for a OneCloud cluster.

    Args:
        client: OneCloud API client.
        cluster_id: Cluster ID from POST /clusters response.
        site: Site code (e.g. POK).
        vlan: VLAN ID the VMs belong to (from VM network.vlan).

    Returns:
        Floating IP address if returned by API, else None.
    """
    body = {"VLAN": int(vlan), "site": site}
    resp = client.put(f"/clusters/{cluster_id}/floating-ip/provision", json=body)
    if resp.status_code not in (200, 201):
        raise NodeError(
            f"OneCloud: failed to provision floating IP for cluster {cluster_id}: "
            f"{resp.status_code} {resp.text[:200]}"
        )
    data = resp.json()
    LOG.info(
        "OneCloud: floating IP provision response for cluster %s: %s", cluster_id, data
    )
    ip = None
    if isinstance(data, dict):
        ip = data.get("floating_ip") or data.get("ip") or data.get("ipaddr")
    return ip


def floating_ip_release(client, cluster_id: int) -> None:
    """
    Release floating IP from a OneCloud cluster.

    Args:
        client: OneCloud API client.
        cluster_id: Cluster ID.
    """
    resp = client.put(f"/clusters/{cluster_id}/floating-ip/release", json={})
    if resp.status_code not in (200, 201, 204):
        raise NodeError(
            f"OneCloud: failed to release floating IP for cluster {cluster_id}: "
            f"{resp.status_code} {resp.text[:200]}"
        )
    LOG.info("OneCloud: floating IP released for cluster %s", cluster_id)


def floating_ip_get(client, cluster_id: int) -> Optional[str]:
    """
    Get the floating IP assigned to a OneCloud cluster.

    Args:
        client: OneCloud API client.
        cluster_id: Cluster ID.

    Returns:
        Floating IP string, or None if not provisioned.
    """
    resp = client.get("/clusters")
    if resp.status_code != 200:
        LOG.warning("OneCloud: GET /clusters failed: %s", resp.status_code)
        return None
    data = resp.json()
    clusters = data.get("data", []) if isinstance(data, dict) else data
    for c in clusters:
        if c.get("clusterid") == cluster_id or c.get("clusterID") == cluster_id:
            return c.get("floating_ip")
    return None


def vm_resize(
    client,
    vmid: int,
    cpu: int,
    mem: int,
    disk_total_gb: int,
    justification: str = "CephCI resize",
) -> None:
    """
    Resize a VM (CPU, memory, total disk).

    Args:
        client: OneCloud API client.
        vmid: VM ID.
        cpu: Number of vCPUs (max 8).
        mem: Memory in GB (max 32).
        disk_total_gb: Total disk size in GB (must be >= current, max 1024).
        justification: Reason for resize.
    """
    if cpu > 8 or mem > 32 or disk_total_gb > 1024:
        raise ValueError(
            f"Resource limits exceeded (cpu<=8, mem<=32GB, disk<=1024GB): "
            f"cpu={cpu}, mem={mem}, disk={disk_total_gb}GB"
        )
    body = {
        "justification": justification,
        "cpu": int(cpu),
        "mem": int(mem),
        "disk": [int(disk_total_gb)],
    }
    resp = client.put(f"/vm/{vmid}/resize", json=body)
    if resp.status_code not in (200, 201):
        raise NodeError(
            f"OneCloud: failed to resize VM {vmid}: "
            f"{resp.status_code} {resp.text[:300]}"
        )
    LOG.info(
        "OneCloud: resize request created for VM %s (cpu=%s, mem=%s, disk=%sGB)",
        vmid,
        cpu,
        mem,
        disk_total_gb,
    )


def vm_get_details(client, vmid: int) -> Dict:
    """
    Get VM details from OneCloud API.

    Args:
        client: OneCloud API client.
        vmid: VM ID.

    Returns:
        VM dict with resources, network, cluster info.
    """
    resp = client.get(f"/vm/{vmid}")
    if resp.status_code != 200:
        raise NodeError(f"OneCloud: GET /vm/{vmid} failed: {resp.status_code}")
    data = resp.json()
    if isinstance(data, dict):
        vms = data.get("data", [data])
        if isinstance(vms, list) and vms:
            return vms[0]
    return data


class CephVMNodeOneCloud:
    """Represents a VM node from OneCloud API."""

    def __init__(
        self,
        node: Dict[str, Any],
        api_key: str,
        base_url: str,
        verify_ssl: bool = False,
    ) -> None:
        """
        Initializes the instance using VM data from OneCloud API.

        Args:
            node: VM dict from GET /vm or GET /vm/{id} (vmid, vmname, network, etc.).
            api_key: JWT for API calls (e.g. delete).
            base_url: API base URL from credentials.
            verify_ssl: If False, disable SSL certificate verification for API calls.
        """
        self.node = node
        self._api_key = api_key
        self._base_url = base_url
        self._verify_ssl = verify_ssl
        self._roles: List = []
        self._subnet: str = ""
        self.root_login: bool = True
        self.osd_scenario: Optional[str] = None
        self.location: Optional[str] = None
        self.id: Optional[str] = None

    @property
    def ip_address(self) -> str:
        """Return the IP address of the node (public/floating IP for SSH)."""
        return get_vm_ip(self.node) or ""

    @property
    def hostname(self) -> str:
        """Return the hostname of the VM."""
        net = self.node.get("network") or {}
        return (
            net.get("hostname")
            or net.get("fqdn", "").split(".")[0]
            or self.node.get("vmname", "")
        )

    @property
    def shortname(self) -> str:
        """Return the short form of the hostname."""
        return self.hostname.split(".")[0] if self.hostname else ""

    @property
    def subnet(self) -> str:
        """Return the subnet CIDR (e.g. 9.114.200.0/24) derived from VM IP."""
        if self._subnet:
            return self._subnet
        ip = self.ip_address
        if not ip:
            return ""
        net = self.node.get("network") or {}
        netmask = net.get("netmask") or net.get("subnet_mask") or ""
        try:
            if netmask:
                iface = ipaddress.IPv4Interface(f"{ip}/{netmask}")
            else:
                iface = ipaddress.IPv4Interface(f"{ip}/24")
            self._subnet = str(iface.network)
            return self._subnet
        except (ValueError, TypeError):
            return ""

    @property
    def no_of_volumes(self) -> int:
        """Return the number of volumes attached to the VM."""
        res = self.node.get("resources") or {}
        disk = res.get("disk") or []
        return len(disk) if isinstance(disk, list) else 0

    @property
    def volumes(self) -> List:
        """Return the list of storage volumes (OneCloud does not expose volume details)."""
        return []

    @property
    def role(self) -> List:
        """Return the Ceph roles of the instance."""
        return self._roles

    @role.setter
    def role(self, roles: list) -> None:
        """Set the roles for the VM."""
        self._roles = deepcopy(roles)

    @property
    def node_type(self) -> str:
        """Return the provider type."""
        return "onecloud"

    @property
    def vmid(self) -> Optional[int]:
        """Return the OneCloud VM ID."""
        return self.node.get("vmid") or self.node.get("vmID")

    def delete(self) -> None:
        """Delete the VM via OneCloud API."""
        vmid = self.vmid
        if vmid is None:
            raise NodeDeleteFailure("Cannot delete VM: no vmid")

        client = get_onecloud_client(
            self._api_key, self._base_url, verify_ssl=self._verify_ssl
        )
        resp = client.delete(f"/vm/{vmid}")
        if resp.status_code not in (200, 204):
            raise NodeDeleteFailure(
                f"Failed to delete VM {vmid}: {resp.status_code} {resp.text[:200]}"
            )

    def start(self, wait: bool = True) -> None:
        """
        Start this VM.

        Args:
            wait: If True, poll until VM reaches running state.
        """
        vmid = self.vmid
        if vmid is None:
            raise NodeError("Cannot start VM: no vmid")
        client = get_onecloud_client(
            self._api_key, self._base_url, verify_ssl=self._verify_ssl
        )
        vm_start(client, vmid)
        if wait:
            _wait_until_vm_state(client, vmid, "on")

    def stop(self, wait: bool = False) -> None:
        """
        Stop this VM.

        Args:
            wait: If True, poll until VM reaches stopped state.
        """
        vmid = self.vmid
        if vmid is None:
            raise NodeError("Cannot stop VM: no vmid")
        client = get_onecloud_client(
            self._api_key, self._base_url, verify_ssl=self._verify_ssl
        )
        vm_stop(client, vmid)
        if wait:
            _wait_until_vm_state(client, vmid, "off")

    def restart(self, wait: bool = True) -> None:
        """
        Restart this VM.

        Args:
            wait: If True, poll until VM reaches running state after restart.
        """
        vmid = self.vmid
        if vmid is None:
            raise NodeError("Cannot restart VM: no vmid")
        client = get_onecloud_client(
            self._api_key, self._base_url, verify_ssl=self._verify_ssl
        )
        vm_restart(client, vmid)
        if wait:
            _wait_until_vm_state(client, vmid, "on")

    @property
    def cluster_id(self) -> Optional[int]:
        """Return the cluster ID this VM belongs to."""
        cluster = self.node.get("cluster") or {}
        cid = cluster.get("id") or cluster.get("clusterid") or cluster.get("clusterID")
        if cid is not None:
            try:
                return int(cid)
            except (TypeError, ValueError):
                pass
        return None

    @property
    def vlan(self) -> Optional[int]:
        """Return the VLAN this VM is on (from network.vlan)."""
        net = self.node.get("network") or {}
        v = net.get("vlan")
        if v is not None:
            try:
                return int(v)
            except (TypeError, ValueError):
                pass
        return None

    @property
    def floating_ip(self) -> Optional[str]:
        """Return the floating IP assigned to this VM's cluster, or None."""
        cid = self.cluster_id
        if cid is None:
            return None
        client = get_onecloud_client(
            self._api_key, self._base_url, verify_ssl=self._verify_ssl
        )
        return floating_ip_get(client, cid)

    def provision_floating_ip(self, site: str = "POK") -> Optional[str]:
        """
        Provision a floating IP for this VM's cluster.

        Args:
            site: Site code (default: POK).

        Returns:
            Floating IP address if returned, else None.
        """
        cid = self.cluster_id
        v = self.vlan
        if cid is None:
            raise NodeError("Cannot provision floating IP: no cluster_id")
        if v is None:
            raise NodeError("Cannot provision floating IP: no VLAN on VM")
        client = get_onecloud_client(
            self._api_key, self._base_url, verify_ssl=self._verify_ssl
        )
        return floating_ip_provision(client, cid, site, v)

    def release_floating_ip(self) -> None:
        """Release floating IP from this VM's cluster."""
        cid = self.cluster_id
        if cid is None:
            raise NodeError("Cannot release floating IP: no cluster_id")
        client = get_onecloud_client(
            self._api_key, self._base_url, verify_ssl=self._verify_ssl
        )
        floating_ip_release(client, cid)

    def resize(
        self,
        cpu: Optional[int] = None,
        mem: Optional[int] = None,
        disk_total_gb: Optional[int] = None,
        justification: str = "CephCI resize",
    ) -> None:
        """
        Resize this VM (CPU, memory, disk).

        Args:
            cpu: vCPUs (default: keep current).
            mem: Memory in GB (default: keep current).
            disk_total_gb: Total disk in GB (default: keep current, must be >= current).
            justification: Reason for resize.
        """
        vmid = self.vmid
        if vmid is None:
            raise NodeError("Cannot resize VM: no vmid")
        client = get_onecloud_client(
            self._api_key, self._base_url, verify_ssl=self._verify_ssl
        )
        details = vm_get_details(client, int(vmid))
        resources = details.get("resources") or {}
        _cpu = cpu if cpu is not None else resources.get("cpu", 4)
        _mem = mem if mem is not None else resources.get("mem", 16)
        current_disks = resources.get("disk") or []
        current_total = sum(current_disks) if current_disks else 100
        _disk = disk_total_gb if disk_total_gb is not None else current_total
        if _disk < current_total:
            raise ValueError(
                f"Disk shrinking not allowed: requested {_disk}GB "
                f"< current {current_total}GB"
            )
        vm_resize(client, int(vmid), _cpu, _mem, _disk, justification)

    def get_details(self) -> Dict:
        """Refresh and return full VM details from the API."""
        vmid = self.vmid
        if vmid is None:
            raise NodeError("Cannot get details: no vmid")
        client = get_onecloud_client(
            self._api_key, self._base_url, verify_ssl=self._verify_ssl
        )
        details = vm_get_details(client, int(vmid))
        self.node.update(details)
        return details
