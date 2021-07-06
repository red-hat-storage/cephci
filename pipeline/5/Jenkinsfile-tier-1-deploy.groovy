/*
    Pipeline script for executing Tier 1 Deployment test suites for RH Ceph 5.x.

    This job involves executing,
        1) Bootstrap scenarios test suites
        2) Upgrade test suites,
            (a) Build to Build(pacific)
            (b) 4x(Nautilus) to 5x(Pacific) Ceph Upgrade
        3) Cephadm orchestration test suites,
            (a) Deploy services using Orchestration commands.
            (b) Deploy Services using specification file(s).
*/

// Global variables section
def nodeName = "centos-7"
def cephVersion = "pacific"
def sharedLib

def testStages0 = [
    'Upgrade_4x_RPM_to_5x_Containerized': {
        stage('Upgrade suite - 4x RPM to 5x containerized version') {
            withEnv([
                "sutVMConf=conf/inventory/rhel-8.4-server-x86_64.yaml",
                "sutConf=conf/${cephVersion}/upgrades/upgrade_from4x_big_cluster.yml",
                "testSuite=suites/${cephVersion}/upgrades/tier-1-upgrade_4x_to_5_x_baremetal.yaml",
                "addnArgs=--post-results --log-level debug"
            ]) {
                sharedLib.runTestSuite()
            }
        }
    },
    'Upgrade_4x_Containerized_to_5x_Containerized': {
        stage('Upgrade suite - 4x containerized to 5x containerized version') {
            sleep(300)
            withEnv([
                "sutVMConf=conf/inventory/rhel-8.4-server-x86_64.yaml",
                "sutConf=conf/${cephVersion}/upgrades/upgrade_from4x_small_cluster.yml",
                "testSuite=suites/${cephVersion}/upgrades/tier-1-upgrade_4x_to_5_x_containerized.yaml",
                "addnArgs=--post-results --log-level debug"
            ]) {
                sharedLib.runTestSuite()
            }
        }
    }
]

def testStages1 = [
    'Bootstrap_Custom_SSL_port_and_Apply_Spec': {
        stage('Bootstrap suite - custom SSL Dashboard port and apply-spec') {
            withEnv([
                "sutVMConf=conf/inventory/rhel-8.4-server-x86_64.yaml",
                "sutConf=conf/${cephVersion}/cephadm/tier1_3node_cephadm_bootstrap.yaml",
                "testSuite=suites/${cephVersion}/cephadm/tier1_ssl_dashboard_port.yaml",
                "addnArgs=--post-results --log-level debug"
            ]) {
                sharedLib.runTestSuite()
            }
        }
    },
    'Bootstrap_skip_dashboard_and_custom_ceph_config_organisation': {
        stage('Bootstrap suite - Skip Dashboard and custom Ceph directory organisation') {
            sleep(180)
            withEnv([
                "sutVMConf=conf/inventory/rhel-8.4-server-x86_64.yaml",
                "sutConf=conf/${cephVersion}/cephadm/tier1_3node_cephadm_bootstrap.yaml",
                "testSuite=suites/${cephVersion}/cephadm/tier1_skip_dashboard.yaml",
                "addnArgs=--post-results --log-level debug"
            ]) {
                sharedLib.runTestSuite()
            }
        }
    },
    'Cephadm_Apply_Spec': {
        stage('Cephadm suite - Apply Service spec File') {
            sleep(360)
            withEnv([
                "sutVMConf=conf/inventory/rhel-8.4-server-x86_64.yaml",
                "sutConf=conf/${cephVersion}/cephadm/tier1_3node_cephadm_bootstrap.yaml",
                "testSuite=suites/${cephVersion}/cephadm/tier1_service_apply_spec.yaml",
                "addnArgs=--post-results --log-level debug"
            ]) {
                sharedLib.runTestSuite()
            }
        }
    },
    'Cephadm_Upgrade_Build_to_Build_5x': {
        stage('Upgrade suite - Build to Build(B2B)') {
            sleep(360)
            withEnv([
                "sutVMConf=conf/inventory/rhel-8.4-server-x86_64.yaml",
                "sutConf=conf/${cephVersion}/cephadm/tier1_3node_cephadm_bootstrap.yaml",
                "testSuite=suites/${cephVersion}/cephadm/tier1_cephadm_upgrade.yaml",
                "addnArgs=--post-results --log-level debug"
            ]) {
                sharedLib.runTestSuite()
            }
        }
    }
]

// Pipeline script entry point
node(nodeName) {

    timeout(unit: "MINUTES", time: 30) {
        stage('Install prereq') {
            checkout([
                $class: 'GitSCM',
                branches: [[name: '*/master']],
                doGenerateSubmoduleConfigurations: false,
                extensions: [[
                    $class: 'SubmoduleOption',
                    disableSubmodules: false,
                    parentCredentials: false,
                    recursiveSubmodules: true,
                    reference: '',
                    trackingSubmodules: false
                ]],
                submoduleCfg: [],
                userRemoteConfigs: [[
                    url: 'https://github.com/red-hat-storage/cephci.git'
                ]]
            ])
            sharedLib = load("${env.WORKSPACE}/pipeline/vars/common.groovy")
            sharedLib.prepareNode()
        }
    }

    timeout(unit: "MINUTES", time: 180) {
        parallel testStages0
    }

    timeout(unit: "MINUTES", time: 120) {
        parallel testStages1
    }

}
