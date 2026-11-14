# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

CephCI is Red Hat/IBM's system test framework for Red Hat Ceph Storage. It is **not** a unit-test
suite: `run.py` provisions real cluster nodes on a cloud (OpenStack, IBM VPC, AWS, baremetal,
onecloud, ocpvirt), SSHes into them, and drives test modules declared in YAML suite files. The
unit tests under `unittests/` only cover framework helpers.

## Commands

```bash
# Full CI gate (what GitHub Actions runs; Python 3.12)
tox -e py

# Individual gate steps
pytest unittests                                   # framework unit tests
pytest unittests/utility/test_utils.py             # single file
pytest unittests/utility/test_utils.py::test_name  # single test
flake8                                             # max-line-length 120, see tox.ini
isort --profile black -c .                         # check; drop -c to fix
black --check --diff .                             # drop flags to fix
yamllint -d relaxed --no-warnings .                # all suite/conf YAML

# Local setup
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt     # runtime; test_requirements.txt adds linters/pytest

# Run a test suite against a real cluster (needs ~/.cephci.yaml + osp creds)
python run.py --product ibm --release 9.2 --platform rhel-10 \
  --build nightly \
  --cloud openstack --osp-cred osp/osp-cred-ci-2.yaml \
  --global-conf conf/tentacle/cephadm/1admin-4node-1client-cluster.yaml \
  --inventory conf/inventory/rhel-10-latest.yaml \
  --suite suites/tentacle/cephadm/tier0-cephadm.yaml \
  --log-level debug

python run.py --help          # docopt usage block lives at the top of run.py
python run.py --store ...     # pickle cluster state into rerun/
python run.py --reuse rerun/<file> ...   # re-run tests on an existing cluster (big time saver)
python run.py --cleanup <instances-name> --osp-cred <file>   # delete leaked VMs/volumes
```

`~/.cephci.yaml` (copy from `cephci.yaml.template`) holds Polarion / Report Portal / CDN /
container-registry credentials. Never commit real credentials; `.gitleaks.toml` is in place.

## Architecture

### Identifying the build under test: product / release / platform

`run.py` resolves *what* to install from four arguments, not from hard-coded URLs:

- **`--product`** — edition of Ceph: `community`, `redhat` (default) or `ibm`. Forced to
  `community` when `--upstream-build` is passed, and to `ibm` by `--custom-config ibm-build=true`.
- **`--release`** — the product version (`8.1`, `9.2`, ...), or the upstream name (`reef`,
  `squid`, `tentacle`) for `community`, which `CephTestManifest.RELEASE_MAP` folds back to
  7/8/9. `--rhbuild` is the deprecated spelling and is still accepted as a fallback.
- **`--platform`** — base OS, e.g. `rhel-9`, `rhel-10`. Must match the `--inventory` image.
- **`--build`** — which section of the manifest to read: `released` (default, silently falls back
  to `nightly` when absent), `nightly`/`latest`, `stable`/`sanity`, `zX`, `cvp`, `upstream`.

Those four construct a `CephTestManifest` (`cephci/utils/build_info.py`), which fetches the
manifest from the external `ibmstorage/qe-ceph-manifest` repo and yields the compose repository,
`repo_id`, container registry/image/tag and ceph version for the run. `--rhs-ceph-repo`,
`--docker-registry`, `--docker-image` and `--docker-tag` override individual manifest values.

Note the directories under `conf/` and `suites/` are named by **upstream** release (`squid`,
`tentacle`, ...) while `--release` for `redhat`/`ibm` is the numeric product version — e.g.
`--release 8.x` runs the `squid` trees, `--release 9.x` the `tentacle` trees.

### The three YAML inputs

Every run is defined by three files, and understanding their split is the key to this repo:

1. **`--global-conf conf/<release>/...`** — cluster layout. Lists `node1..nodeN` with `role`
   (`_admin`, `installer`, `mon`, `mgr`, `osd`, `rgw`, `mds`, `client`, ...), volume count, disk
   size, networks. Roles drive `Ceph.get_nodes(role)` / `get_ceph_object(role)` lookups in tests.
   (`--cluster-conf` is the UPI/baremetal equivalent with real hostnames and SSH keys.)
2. **`--inventory conf/inventory/...`** — OS image / flavor per cloud.
3. **`--suite suites/<release>/...`** — ordered list of tests. Each entry is
   `test: {name, desc, module, polarion-id, config, abort-on-fail, destroy-cluster, clusters, parallel}`.

Suites and confs are organized by Ceph upstream release name (`pacific`, `quincy`, `reef`,
`squid`, `tentacle`, `umbrella`) and filenames encode tier (`tier0-...`, `tier1-...`).

### Suite composition (`init_suite.py`)

`--suite` is repeatable and may point at a **directory** instead of a file. A directory containing
a (symlinked) suite yaml plus `overrides.yaml` gets dict-merged at load time: `overrides.yaml` can
patch a test by `index`, add keys, or wrap every test's `config` under named entries in a
`clusters:` list (used for multi-cluster RGW multisite / rbd-mirror suites). Read the `__collate`
docstring in `init_suite.py` before touching this. Prefer symlinks over copying YAML between dirs.

### Test module contract (`tests/`)

`run.py` appends most `tests/*` subdirectories to `sys.path`, so `module: test_mon.py` in a suite
resolves by bare filename — **module filenames must stay unique across `tests/`**. Each module
exposes:

```python
def run(ceph_cluster, **kw) -> int:   # 0 = Pass, -1 = Skipped, anything else = Failed
```

`kw` carries `config` (the suite's `config` block, enriched by `run.py` with `product`,
`release`, `platform` and the `manifest` object, plus `rhbuild` — which is the
`"<release>-<platform>"` string, not the bare version — `base_url`, `container_image`,
`cloud-type`, custom-config flags, ...), `test_data`
(dict shared across tests in a run — use it to pass state), `ceph_cluster_dict`, `clients`,
`run_config`, and `parallel`. Raising propagates as a failure with a traceback; `abort-on-fail`
in the suite stops the run.

Error text surfaced in reports is scraped from the module-level `Log` object's `_log_errors`, so
tests must use `log = Log(__name__)` from `utility.log` rather than `print` or stdlib logging.

### Layers for driving Ceph

- **`ceph/ceph.py`** — `Ceph`, `CephNode`, `CephObject`/`CephDemon` and `SSHConnectionManager`.
  The node-level primitive is `node.exec_command(sudo=..., cmd=..., check_ec=...)`.
- **`ceph/ceph_admin/`** — mixin-based cephadm/orchestrator wrapper. `CephAdmin` composes
  `BootstrapMixin`, `ShellMixin`, `RegistryLoginMixin`; per-service classes (`Mon`, `Osd`, `Rgw`,
  `Nfs`, ...) compose verb mixins (`apply`, `add`, `remove`, `restart`, `ls`, `ps`, ...).
  Tests dispatch dynamically via `fetch_method(obj, config.pop("command"))`, which is why suite
  YAML reads `service: host` / `command: add` / `args: {...}`; `config_dict_to_string` turns
  `args` into CLI flags.
- **`cli/`** — newer, thinner command-builder layer. `cli.Cli.execute` runs a command on one node
  or fans out over a list; subclasses (`CephAdm`, `Ceph`, ...) build command strings with
  `build_cmd_from_args`. New CLI-driven tests generally prefer this over `ceph_admin`.
- **`rest/`, `api/`** — REST/dashboard API endpoints and workflows.

### Provisioning and lifecycle

`compute/` has one module per cloud backend (`openstack` via libcloud, `ibm_vpc`, `aws_ec2`,
`baremetal`, `onecloud`, `openshift` for ocpvirt), each exposing create and
`cleanup_*_ceph_nodes`. `cephci/` holds the newer provisioning/cleanup/report helpers
(`provision.py`, `cleanup.py`, `cluster_conf.py`, `cluster_info.py`, `utils/build_info.py` ->
`CephTestManifest`, which resolves the build/compose under test).

Results go out via `utility/polarion.py` (`--post-results`), Report Portal, `utility/xunit.py`
(`--xunit-results`), and e-mail. `pipeline/*.groovy` are the Jenkins jobs that invoke `run.py`.

## Conventions

- Docstrings are mandatory at module/class/function level with documented params, returns and
  raised exceptions (see `docs/source/contribution_guidelines.rst`).
- Absolute imports only, grouped stdlib / third-party / local. Avoid `*args, **kwargs` in new
  helper signatures; prefer keyword arguments at call sites (`wait_till(timeout=300, polling=10)`).
- YAML: lowercase filenames, `.yaml` (not `.yml`), `-` separates within a category and `_`
  between categories.
- Commits need a DCO `Signed-off-by` line (`git commit -s`); PRs target `main` and are reviewed by
  the team listed in `.github/CODEOWNERS` / per-directory `OWNERS` files.
- Run `tox` before pushing — linting is part of the required CI check.
