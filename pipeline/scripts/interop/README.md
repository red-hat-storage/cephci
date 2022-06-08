## Interop Test suite
The test suites in this folder are specifically created to be executed in RHEL
interop pipeline. They are derived from the existingRed Hat Ceph Storage tier-0
suites.

They differ from them on the following points

- Platform is dynamic and based on the passed inventory
- Accepts VM spec file
- Maybe a subset of the tier-0
- A single set of scripts for the Major version.

### CLI Arguments

| Options                           | Description |
|:----------------------------------| ----------- |
| `--osp-cred <cred.yaml>`          | OpenStack credential file |
| `--add-repo <repo.repo>`          | Additional repository file to be used. |
| `--inventory <vm_spec.yaml>`      | VM spec file to be used for deployment.|
| `--platform rhel-<major-version>` | Operating System deployed in the systems.|

### Environment Variables

| Variable | Description |
| -------- | ----------- |
| CEPH_PLATFORM | Operating System deployed in the systems |
| PY_CMD | Python command to be used for execution. |
| OSP_CRED_FILE | Absolute path to OpenStack credential |


__Examples___
```usage
# Defaults
bash pipeline/scripts/interop/test-ceph-features.sh \
    --osp-cred cred.yaml \
    --add-repo repo.repo \
    --inventory test.yaml \
    --platform rhel-<major-version>

# Custom environment
export PY_CMD=/home/fedora/cephci-env/bin/python
bash pipeline/scripts/interop/test-ceph-features.sh \
    --osp-cred rhos-d.yaml \
    --add-repo file.repo \
    --inventory rhel-8.5-latest.yaml \
    --platform rhel-8
```

It is recommended to execute the script from the root folder of this repo.
This enables the script to execute `run.py` from a relative path.
