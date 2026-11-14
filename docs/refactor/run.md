# Restructuring `run.py`

`run.py` is the single entry point for every CephCI execution: Jenkins builds a command
line in `pipeline/vars/*.groovy` and invokes `python run.py ...`. Over time the module
accumulated argument parsing, cloud provisioning, build resolution, test execution,
reporting and artifact collection in one 1443-line file whose `run()` function alone was
825 lines.

This document is the plan of record for breaking it apart, and a map of where the code
lives now that the work is done.

## Why

| Problem | Evidence (pre-refactor line numbers) |
| ------- | ------------------------------------ |
| `run()` is one ~825-line function | `run.py:535-1360`, with a closure `fetch_test_details` at `:824` capturing ~15 locals |
| Cloud dispatch duplicated five times | create `:262-298`, per-node credentials `:330-372`, pre-create cleanup `:244-257`, `--cleanup` `:601-621`, `destroy-cluster` `:1225-1245` |
| Argument handling scattered | ~45 `args.get()` calls spread across `:540-756`, with four unrelated validation guards interleaved at `:625-645` |
| Test modules resolved by mutating `sys.path` | 22 `sys.path.append()` calls at `:926-947`; modules load by bare filename, so every filename under `tests/` must be globally unique, and a module imported both ways (`test_host` and `tests.cephadm.test_host`) is loaded twice with separate state |
| Result payload built twice | provisioning-failure path `:886-907` duplicates the normal path `:1295-1317` |
| Error text gathered by introspection | `:1174-1186` scans `vars(test_mod)` for a `Log` instance and reads its private `_log_errors` |
| Cloud-specific retry on generic code | `@retry(LibcloudError, ...)` at `:197` wraps provisioning for every cloud, including baremetal |
| Dead code | `--report-portal` is read at `:570` but absent from the docopt spec, so it is always `None`; `run_summary` is declared at `:190`, never written, and dumped to `run_summary.json` at `:1304`; `collect_coredump` initialised twice (`:662`, `:748`); `build`/`base_url`/`docker_*` set to `None` at `:590-594` then immediately overwritten |
| Version scrape in the wrong loop | `:800-813` performs an HTTP GET plus regex once per cluster in `conf["globals"]`, yielding the same answer each time |

## Target layout

`run.py` stays at the repository root — Jenkins invokes it by path — and is now a thin
shim over the package below.

```
cephci/orchestration/
    doc.py          # docopt usage block: the frozen CLI contract
    config.py       # RunConfig: docopt dict -> validated settings object
    context.py      # RunContext: run id, run dir, logging, timing, metadata
    build.py        # resolve(): product/release/platform/build -> BuildArtifacts
    provisioning.py # cluster creation and teardown over the provider registry
    loader.py       # suite `module:` -> imported Python module
    execution.py    # per-test config assembly and invocation
    results.py      # RunResults: result records and the e-mail/xunit payloads
    artifacts.py    # coredumps, sosreports, /var/log/ceph
    console.py      # human-readable result table
    runner.py       # phase orchestration (the former `run()`)
cephci/providers/
    base.py         # CloudProvider protocol
    openstack.py ibmc.py aws.py baremetal.py onecloud.py ocpvirt.py
    __init__.py     # name -> provider registry
```

## Where the tests live

| Area | Tests |
| ---- | ----- |
| Command line and validation | `unittests/orchestration/test_config.py` |
| Build resolution | `unittests/orchestration/test_build.py` |
| Provisioning over a provider | `unittests/orchestration/test_provisioning.py` |
| Providers and the registry | `unittests/providers/test_providers.py` |
| Per-test configuration and execution | `unittests/orchestration/test_execution.py` |
| Module resolution | `unittests/orchestration/test_loader.py` |
| Result records and payloads | `unittests/orchestration/test_results.py` |
| Phase orchestration, end to end | `unittests/orchestration/test_runner.py` |

## Phases

Each phase was independently shippable; none required the next one to land.

### Phase 0 — safety net

`run.py` had no test coverage. Characterization tests now parse the real command lines
used by the Jenkins libraries in `pipeline/` and assert the settings they produce. These
are the regression oracle for every later phase.

* `unittests/orchestration/test_config.py`

### Phase 1 — lift pure helpers

Mechanical moves, no behaviour change: `print_results` to `console.py`, `load_file` and
`get_html_page` to `cephci/utils/utility.py`, `store_cluster_state` to `results.py`,
`collect_recipe` to `cephci/cluster_info.py` alongside its siblings. `get_tier_level`
was defined but never called anywhere in the repository, so it was deleted rather than
moved.

### Phase 2 — `RunConfig`

One frozen dataclass built in `config.py` from the docopt dictionary, with the
validation rules collected into `validate_cleanup()` and `validate_execution()` — kept
apart because a cleanup only invocation deliberately skips the environment checks.
After this phase nothing downstream reads `args`. The dead `--report-portal` read and
the duplicated initialisations are gone; `run_summary.json` is still written (empty)
because the pipelines may collect it.

`--report-portal` deserves a note. `run.py` read it, but it was never part of the
docopt usage block, so docopt rejects any command line that passes it. The option is
therefore unusable, and `pipeline/vars/common.groovy:178` appends it in `runTestSuite`,
which means that function cannot have run successfully for some time. Nothing else
references `runTestSuite`. Deciding whether to restore the option or delete the Groovy
function is left to the pipeline owners; the refactor removed only the dead read.

### Phase 3 — `CloudProvider`

A provider exposes `create(cluster, inventory, run_id, prefix)`, `cleanup(prefix)`,
`login(node)` and `retry_exceptions`, and is looked up in a registry keyed by the
`--cloud` value. The five duplicated `if/elif` chains collapse to one dispatch, the
libcloud retry moves to the providers that can raise it, and adding a cloud is a
one-file change.

### Phase 4 — build resolution

`build.resolve()` returns a `BuildArtifacts`, encapsulating the `released` to `nightly`
fallback, `CephTestManifest` construction, and the `--rhs-ceph-repo`/`--docker-*`
override check. The ceph-version scrape is hoisted out of the per-cluster loop and runs
once.

### Phase 5 — execution

`TestExecutor.execute()` returns an `ExecutionOutcome`, and the ~60-line `config[...]`
enrichment becomes the pure, testable `build_test_config()`. The `_log_errors`
introspection is preserved but isolated behind `collect_module_errors()`.

`TestModuleLoader` owns the import path. The plan was to drop the `sys.path` mutation
entirely; that turned out to be impossible. Around 165 modules under `tests/nfs`,
`tests/smb` and their neighbours import a sibling by bare name
(`from nfs_operations import ...`), which only resolves because those directories are
on `sys.path`. The loader therefore still installs the same 22 directories, in the same
order, and the list is a named constant with the ordering constraint written down.

What the loader adds is diagnosis: before importing a bare name it checks how many
files could satisfy it. Four names are ambiguous today — `test_cephadm_ansible.py`,
`test_nvmeof_rest_sanity.py`, `exceptions.py` and `__init__.py` — so an ambiguous name
logs a warning naming every candidate and the one that won, rather than raising. Making
it fatal would break the suites that depend on the current resolution order.

### Phase 6 — results and artifacts

Both the early-failure path and the normal path feed one `RunResults`, so the e-mail and
xunit payloads are constructed in a single place. `ArtifactCollector` owns coredump,
sosreport and ceph-log collection.

### Phase 7 — module resolution and deprecations

`module:` entries accept a dotted path (`tests.cephadm.test_host`) in addition to the
legacy bare filename (`test_host.py`). A dotted reference names exactly one file, so it
is immune to the ambiguity described above. `--rhbuild`, `--upstream-build` and
`--custom-config ibm-build=true` log deprecation warnings pointing at
`--release`/`--product`; all three keep working.

The ~34,000 `module:` references in `suites/` were **not** rewritten. Both spellings are
supported indefinitely; new suites should prefer the dotted form, which is unambiguous
and lets editors and linters follow the reference.

## Invariants

These are contracts with Jenkins and with on-disk state. Any change to them needs a
matching change in `pipeline/`.

* **CLI surface.** `pipeline/vars/common.groovy` and friends build command strings
  textually. No flag may be renamed or removed without updating them. `doc.py` holds the
  usage block verbatim.
* **Exit codes.** `run()` returns 0 or 1. Test modules return `0` (Pass), `-1` (Skipped)
  or anything else (Failed); `-1` is also what the runner substitutes for a skipped test.
* **`version_info.json`** is written to the working directory and read back by
  `pipeline/vars/common.groovy:265,277`.
* **Pickle compatibility.** `--store`/`--reuse` serialise live `CephNode` objects, so
  classes in `ceph.ceph` must not move or be renamed. This is the sharpest constraint on
  any future phase.
* **Run directory layout** and the magna URL rewrite for `/ceph/cephci-jenkins` paths.
* **Payload keys** for e-mail, xunit and Polarion.

## Known behaviour preserved deliberately

Carried over unchanged so the refactor stays behaviour-preserving. Each is a candidate
for a follow-up with its own review, and each is pinned by a test so that a future
change to it has to be deliberate.

* Cleanup before creating an environment forwards `--custom-config` only for `onecloud`
  and `ocpvirt`. IBM Cloud and AWS receive `None`, so a run that selects a VPC or region
  through `--custom-config` cleans a different one than it creates in. This is encoded
  as `CUSTOM_CONFIG_AWARE_CLEANUP` in `provisioning.py`.
* `recreate-cluster` creates the new environment without `--custom-config`, unlike the
  initial creation.
* A test that fails on one cluster skips the remaining clusters of that same test.
* `skip_version_compare` set by one test's `config` block stays in effect for every
  later test of the suite.
* Values of `--custom-config` are read with `bool()`, so
  `--custom-config collect-coredump=false` *enables* collection. Any non-empty string
  is true.
* A value containing an equals sign is truncated: `--custom-config url=http://h/a=b`
  yields `http://h/a`.
* `run_summary.json` is always an empty object.
* The e-mail always reports `info.status` as `Pass`; the real verdict is derived from
  the records by `utility.utils.get_run_status`.
* `config["rhbuild"]` is the `"<release>-<platform>"` string, not a bare version.
* `ceph-version-name` is always `None` in the result records.

## Deliberate differences

Three things do not behave identically, all of them cosmetic:

* The wait after creation logs `Sleeping 75 Seconds` rather than a OneCloud specific
  message.
* An unsupported `--cloud` fails before the clusters are iterated rather than during the
  first iteration. The exception type, message and the resulting e-mail are unchanged.
* The OneCloud credential resolution logs once per cluster from the provider rather than
  from the provisioning loop.

## Verification

The refactor cannot be exercised against a real cloud from a workstation, so the
evidence is:

* `pytest unittests` — 257 tests, of which 126 are new, covering argument parsing
  against the command lines the pipelines build, build resolution, provisioning over a
  fake provider, the per-test configuration, module loading, result payloads, and the
  whole runner end to end with the infrastructure stubbed out.
* `flake8`, `isort --profile black -c`, `black --check` over the touched tree.
* A literal by literal comparison of the old `run.py` against the new modules, to
  confirm that every log message, payload key and error string either survived or was
  removed on purpose.
* `python run.py --help` and the cleanup guard exercised by hand.

What remains unverified is the behaviour against live infrastructure: provisioning on
each of the six clouds, the `--store`/`--reuse` round trip against a real cluster, and
the Polarion, Report Portal and e-mail integrations. A tier-0 run per cloud is the
sensible gate before this reaches the pipelines.
