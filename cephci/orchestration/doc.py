"""Command line contract for CephCI.

The usage block below is parsed by docopt and is consumed verbatim by the Jenkins
libraries under ``pipeline/``. Treat it as a frozen interface: flags may be added, but
renaming or removing one breaks the pipelines that build the command line as text.
"""

doc = """
A simple test suite wrapper that executes tests based on yaml test configuration

 Usage:
  run.py (--rhbuild BUILD | --release ceph_release)
        (--platform <name>)
        (--suite <FILE>)...
        (--global-conf FILE | --cluster-conf FILE)
        [--cloud <openstack> | <ibmc> | <aws> | <baremetal> | <onecloud> | <ocpvirt>]
        [--build <name>]
        [--inventory FILE]
        [--osp-cred <file>]
        [--rhs-ceph-repo <repo>]
        [--add-repo <repo>]
        [--kernel-repo <repo>]
        [--store | --reuse <file>]
        [--skip-cluster]
        [--skip-subscription]
        [--docker-registry <registry>]
        [--docker-image <image>]
        [--docker-tag <tag>]
        [--insecure-registry]
        [--post-results]
        [--upstream-build <upstream-build>]
        [--log-level <LEVEL>]
        [--log-dir  <directory-name>]
        [--instances-name <name>]
        [--osp-image <image>]
        [--filestore]
        [--use-ec-pool <k,m>]
        [--hotfix-repo <repo>]
        [--skip-version-compare]
        [--custom-config <key>=<value>]...
        [--custom-config-file <file>]
        [--xunit-results]
        [--enable-eus]
        [--skip-enabling-rhel-rpms]
        [--skip-sos-report]
        [--skip-tc <items>]
        [--monitor-performance]
        [--disable-console-log]
        [--product <community> | <redhat> | <ibm>]
  run.py --cleanup=name --osp-cred <file> [--cloud <str>]
        [--log-level <LEVEL>]
        [--custom-config <key>=<value>]...

Options:
  -h --help                         show this screen
  -v --version                      run version
  -s <smoke> --suite <smoke>        test suite to run
                                    eg: -s smoke or -s rbd
  -f <tests> --filter <tests>       filter tests based on the patter
                                    eg: -f 'rbd' will run tests that have 'rbd'
  --global-conf <file>              global cloud configuration file
  --cluster-conf <file>             cluster configuration file
  --inventory <file>                hosts inventory file
  --cloud <cloud_type>              cloud type (openstack|ibmc|aws|baremetal|onecloud|ocpvirt) [default: openstack]
  --osp-cred <file>                 openstack credentials as separate file
  --rhbuild <1.3.0>                 ceph downstream version
                                    eg: 1.3.0, 2.0, 2.1 etc
                                    Depreciated in favor of --release
  --release <ceph_version>          Ceph version. It can be download stream or
                                    upstream distinct name.
  --build <nightly>                 Type of build to be use for testing
                                    eg: nightly|stable|released|zX
                                    [default: released]
  --upstream-build <upstream-build> eg: quincy|pacific
                                    Depreciated in favor of --release
  --platform <rhel-8>               select platform version eg., rhel-8, rhel-7
  --rhs-ceph-repo <repo>            location of rhs-ceph repo
                                    Top level location of compose
  --add-repo <repo>                 Any additional repo's need to be enabled
  --kernel-repo <repo>              Zstream Kernel Repo location
  --store                           store the current vm state for reuse
  --reuse <file>                    use the stored vm state for rerun
  --skip-cluster                    skip cluster creation from ansible/ceph-deploy
  --skip-subscription               skip subscription manager if using beta rhel images
  --docker-registry <registry>      Docker registry, default value is taken from ansible
                                    config
  --docker-image <image>            Docker image, default value is taken from ansible
                                    config
  --docker-tag <tag>                Docker tag, default value is 'latest'
  --insecure-registry               Disable security check for docker registry
  --post-results                    Post results to polarion, needs Polarion IDs
                                    in test suite yamls. Requires config file, see
                                    README.
  --log-level <LEVEL>               Set logging level
  --log-dir <LEVEL>                 Set log directory
  --instances-name <name>           Name that will be used for instances creation
  --osp-image <image>               Image for osp instances, default value is taken from
                                    conf file
  --filestore                       To specify filestore as osd object store
  --use-ec-pool <k,m>               To use ec pools instead of replicated pools
  --hotfix-repo <repo>              To run sanity on hotfix build
  --skip-version-compare            Skip verification that ceph versions change post
                                    upgrade
  -c --custom-config <name>=<value> Add a custom config key/value to ceph_conf_overrides
                                    Cephadm ODF/Rook-like profile (opt-in via -c)
                                    apply-odf-defaults=true merges
                                    conf/tentacle/rook/odf_rook_defaults.yaml into
                                    bootstrap --config (OSDMap ratios, ms_bind_msgr1=false,
                                    RBD ms_mode); v2-only mon set-addrs runs at end of
                                    bootstrap.py, after test_cephadm deploy,
                                    tests/ceph_installer/deploy_rook_defaults_ceph.py
                                    after mon apply, and in
                                    tests/cephadm/test_mon.py after mon apply.
                                    apply-odf-topology=true is DO-NOT-USE (legacy
                                    zone CRUSH path); use deploy_rook_defaults_ceph.py
                                    for rack+ssd topology instead.
                                    verify-odf-defaults=true checks config/osd/mon dump.
  --custom-config-file <file>       Add custom config yaml to ceph_conf_overrides
  --xunit-results                   Create xUnit result file for test suite run
                                    [default: false]
  --enable-eus                      Enables EUS rpms on EUS suppored distro
                                    [default: false]
  --skip-enabling-rhel-rpms         skip adding rpms from subscription if using beta
                                    rhel images for Interop runs
  --skip-sos-report                 Enables to collect sos-report on test suite failures
                                    [default: false]
  --skip-tc <items>                 skip test case provided in comma seperated fashion
  --monitor-performance             Monitor performance and CPU usage on all/required nodes
                                    for every test and collects data to specified dir
  --disable-console-log             To stopping logging to console
                                    [default: false]
  --product <product>               The edition of Ceph. Accepted values are
                                    community, redhat and ibm
"""
