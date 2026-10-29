---
markdownlint:
  MD013: false
---

# OCP Virtualization (KubeVirt) for cephci

cephci can provision Ceph test clusters on **OpenShift Virtualization** using `--cloud ocpvirt`. VMs are created via the Kubernetes API (KubeVirt + CDI); compute sizing uses cluster **VirtualMachineClusterInstancetype** profiles.

## Configuration overview

Three separate files are used (auth, tenant/cluster settings, and guest image + cloud-init):

| File | Purpose |
|------|---------|
| `--osp-cred` | API authentication only (see `osp-cred-ocpvirt-example.yaml`) |
| `conf/ocpvirt/<name>.yaml` | API server, namespace, storage, network, datasources, root disk size |
| `--inventory` | OS image alias + cloud-init (`conf/inventory/ocpvirt-rhel-*-latest.yaml`) |

Select the namespace template with:

```bash
--custom-config ocpvirt_namespace=rdu3_ceph_jenkins
```

The value must match a file under `conf/ocpvirt/` (without `.yaml`).

## `osp-cred-ocpvirt-example.yaml`

Auth-only credential file. Copy and fill in secrets; do **not** put cluster settings here.

```yaml
globals:
  ocpvirt-credentials:
    token: "<service-account-bearer-token>"
    certificate_authority_data: "<base64 CA from kubeconfig>"   # recommended
    private_key_path: "~/.ssh/id_ed25519"                       # optional, for SSH
```

| Key | Required | Description |
|-----|----------|-------------|
| `token` | Yes | Bearer token for a ServiceAccount with rights to create VMs, DataVolumes, and Secrets in the target namespace |
| `certificate_authority_data` | Recommended | `certificate-authority-data` from the cluster kubeconfig (base64) |
| `ssl_ca_cert` | Alternative to above | Path to a PEM CA bundle |
| `private_key_path` | Optional | SSH private key on the cephci runner; cloud-init also sets password/key auth |

Server URL, namespace, storage class, and network come from `conf/ocpvirt/`, not from this file.

## Namespace template (`conf/ocpvirt/<name>.yaml`)

Example: `conf/ocpvirt/rdu3_ceph_jenkins.yaml`

| Key | Required | Description |
|-----|----------|-------------|
| `server` | Yes | OpenShift API URL, e.g. `https://api.example:6443` |
| `namespace` | Yes | Tenant namespace for VMs and DataVolumes |
| `storage_class` | Yes | StorageClass for root and OSD DataVolumes |
| `network` | Yes | Multus network name, or `default` for pod networking |
| `root_disk_size` | Optional | Root disk size (default `80Gi`) |
| `datasources` | Recommended | Map short inventory names to full CDI DataSource URLs |
| `access_modes` | Optional | PVC access modes (default `ReadWriteOnce`) |
| `subnet` | Optional | Guest subnet hint for cephadm `public_network` auto-detect |

### Datasources map

Inventory can use short names; the namespace file expands them to full URLs:

```yaml
# inventory
image-name: datasource://rhel9

# conf/ocpvirt/rdu3_ceph_jenkins.yaml
datasources:
  rhel9: datasource://openshift-virtualization-os-images/rhel9
  rhel10: datasource://openshift-virtualization-os-images/rhel10
```

At provision time cephci converts the URL into a CDI `sourceRef` on the root DataVolume (clone from cluster `DataSource`).

## Inventory (`conf/inventory/ocpvirt-rhel-*-latest.yaml`)

Contains only:

- `image-name` — usually `datasource://rhel9` or `datasource://rhel10`
- `setup` — cloud-init userdata (users, SSH keys, packages, `ceph-qa-ready` marker)

CPU, memory, and root disk size are **not** in inventory. Sizing uses either `ocpvirt_profile` (instance type) or explicit `ocpvirt_cpu` + `ocpvirt_memory`; root disk size uses the namespace template.

## Instance types (`ocpvirt_profile`)

By default, VM CPU/memory come from a cluster **VirtualMachineClusterInstancetype**:

```bash
# default: o1.large (2 vCPU, 8Gi)
--custom-config ocpvirt_profile=o1.xlarge   # override
```

List available types on the cluster:

```bash
oc get virtualmachineclusterinstancetypes
```

**Recommended on RDU3 tenants:** `o1.*` / `u1.*` (general-purpose, no dedicated CPU). **`cx1.*`** profiles require `cpumanager=true` nodes and may fail to schedule on some tenants.

## Custom CPU / memory (`ocpvirt_cpu` + `ocpvirt_memory`)

When no suitable instance type exists, pass explicit resources instead of a profile. cephci creates the VM with inline KubeVirt `domain.cpu` / `domain.memory` and does **not** set `spec.instancetype`.

It also sets `domain.resources` requests/limits to guest CPU +1 and guest memory +2Gi (KubeVirt overhead). Before creating any PVCs/VMs, cephci checks that the bumped values fit under the namespace LimitRange **max** and fails early with a clear error if not.

```bash
# both required; mutually exclusive with ocpvirt_profile
--custom-config ocpvirt_cpu=4 \
--custom-config ocpvirt_memory=16Gi
# → guest 4 / 16Gi; pod requests+limits 5 CPU / 18Gi
```

| Key | Aliases | Description |
|-----|---------|-------------|
| `ocpvirt_cpu` | `ocpvirt-cpu` | vCPU cores (positive integer) |
| `ocpvirt_memory` | `ocpvirt-memory` | Kubernetes quantity (`16Gi`) or integer GiB (`16` → `16Gi`) |

Rules:

- Both `ocpvirt_cpu` and `ocpvirt_memory` must be set together.
- Do **not** also set `ocpvirt_profile` (clear error).
- When only a profile is set (or neither), behavior is unchanged (`o1.large` default).
- Guest + bump must be ≤ LimitRange max (e.g. max CPU 20 → `ocpvirt_cpu` ≤ 19).

### Suite / job metadata example

For a suite that needs non-standard sizing (e.g. 6 vCPU / 24Gi):

```yaml
# Jenkins / suite custom-config metadata
custom-config:
  - ocpvirt_namespace=rdu3_ceph_jenkins
  - ocpvirt_cpu=6
  - ocpvirt_memory=24Gi
```

Equivalent CLI:

```bash
python3 run.py \
  --cloud ocpvirt \
  --osp-cred /path/to/osp-cred.yaml \
  --custom-config ocpvirt_namespace=rdu3_ceph_jenkins \
  --custom-config ocpvirt_cpu=6 \
  --custom-config ocpvirt_memory=24Gi \
  ...
```

## Steps to generate required configs

### 1. ServiceAccount token

Create or use an existing ServiceAccount in the config namespace with permissions to manage VirtualMachines, DataVolumes, Secrets, and PVCs in the **runtime** namespace.

```bash
# Example: create a long-lived token (adjust SA name/namespace)
oc create token tenantaccess-cephci-executor -n ceph-jenkins--config --duration=8760h
```

Paste the token into `osp-cred` under `globals.ocpvirt-credentials.token`.

### 2. Cluster CA

From the kubeconfig used to reach the API:

```bash
kubectl config view --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}'
```

Set `certificate_authority_data` in the osp-cred file.

Alternatively, save the CA to a file and set `ssl_ca_cert`.

### 3. Namespace template

Gather from your cluster/tenant admin:

- API server URL (`oc whoami --show-server`)
- Runtime namespace (where VMs run)
- `storage_class` for PVCs (e.g. `rh-restricted-nfs`)
- Multus `network` name (e.g. `bridge-504`)

Create `conf/ocpvirt/<your_tenant>.yaml` (copy an existing RDU3 file and edit).

### 4. Datasource URLs

Find OS images in the virtualization namespace:

```bash
oc get datasource -n openshift-virtualization-os-images
```

Add entries under `datasources` in the namespace template:

```yaml
datasources:
  rhel9: datasource://openshift-virtualization-os-images/rhel9
```

### 5. SSH access

- Add the cephci runner's public key to inventory cloud-init `ssh_authorized_keys`
- Set `private_key_path` in osp-cred if using key-based SSH from the runner

### 6. Verify merged config (optional)

```bash
python3 -c "
from pathlib import Path
import yaml
from compute.openshift import resolve_ocpvirt_credentials
cred = resolve_ocpvirt_credentials(
    yaml.safe_load(Path('ocpvirt/osp-cred-ocpvirt-example.yaml').read_text()),
    ['ocpvirt_namespace=rdu3_ceph_jenkins'],
)
print('server:', cred.get('server'))
print('namespace:', cred.get('namespace'))
print('datasources:', cred.get('datasources'))
"
```

## Example run

```bash
python3 run.py \
  --cloud ocpvirt \
  --osp-cred /path/to/osp-rdu3-ceph-jenkins.yaml \
  --custom-config ocpvirt_namespace=rdu3_ceph_jenkins \
  --custom-config pvc_batch_size=3 \
  --custom-config vm_batch_size=1 \
  --custom-config ibm-build=True \
  --instances-name my-bvt-run \
  --rhbuild 8.1 \
  --platform rhel-9 \
  --inventory conf/inventory/ocpvirt-rhel-9-latest.yaml \
  --global-conf conf/compaction/bvt/bvt-3node-1client.yaml \
  --suite suites/compaction/deployments/bvt-deploy-and-configure.yaml \
  --build rc \
  --log-dir /path/to/logs
```

### Useful custom-config keys

| Key | Default | Description |
|-----|---------|-------------|
| `ocpvirt_namespace` | (required) | Basename of `conf/ocpvirt/<name>.yaml` |
| `ocpvirt_profile` | `o1.large` | VirtualMachineClusterInstancetype name (omit when using custom CPU/RAM) |
| `ocpvirt_cpu` | (none) | vCPU cores for inline sizing; requires `ocpvirt_memory` |
| `ocpvirt_memory` | (none) | Memory quantity (e.g. `16Gi`); requires `ocpvirt_cpu` |
| `pvc_batch_size` | `3` | Non-root DataVolumes created per batch |
| `vm_batch_size` | `1` | VirtualMachines created per batch |

## Cleanup

Removes VMs, cloud-init Secrets, and DataVolumes/PVCs matching the instance name:

```bash
python3 run.py \
  --cleanup=<instances-name> \
  --cloud ocpvirt \
  --osp-cred /path/to/osp-cred.yaml \
  --custom-config ocpvirt_namespace=rdu3_ceph_jenkins
```

## Image name flow (reference)

```
inventory:  datasource://rhel9
     ↓  resolve_ocpvirt_image_name (datasources map in conf/ocpvirt)
full URL:   datasource://openshift-virtualization-os-images/rhel9
     ↓  _image_source (CDI sourceRef)
DataVolume: sourceRef → DataSource rhel9 in openshift-virtualization-os-images
```

## Existing namespace templates

| File | `--custom-config ocpvirt_namespace=` |
|------|--------------------------------------|
| `conf/ocpvirt/rdu3_ceph_jenkins.yaml` | `rdu3_ceph_jenkins` |
| `conf/ocpvirt/rdu3_ceph_ci.yaml` | `rdu3_ceph_ci` |
| `conf/ocpvirt/rdu3_ceph_core.yaml` | `rdu3_ceph_core` |
| `conf/ocpvirt/rdu3_ceph_perf.yaml` | `rdu3_ceph_perf` |
| `conf/ocpvirt/rdu3_ceph_sys_test.yaml` | `rdu3_ceph_sys_test` |
