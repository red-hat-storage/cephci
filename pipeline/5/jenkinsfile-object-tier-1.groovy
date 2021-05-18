/*
    Pipeline script for executing Tier 1 object test suites for RH Ceph 5.0.
*/
// Global variables section

def nodeName = "centos-7"
def cephVersion = "pacific"
def sharedLib
def testResults = [:]
def testStages = [
    'Object_versioning_and_dynamic_resharding': {
        stage('Object suite - Basic versioning, dynamic resharding and lifecycle tests') {
            script {
                withEnv([
                    "sutVMConf=conf/inventory/rhel-8.3-server-x86_64.yaml",
                    "sutConf=conf/${cephVersion}/rgw/tier_1_rgw_cephadm.yaml",
                    "testSuite=suites/${cephVersion}/rgw/tier_1_object.yaml",
                    "addnArgs=--post-results --log-level DEBUG"
                ]) {
                    rc = sharedLib.runTestSuite()
                    testResults['Object_basic_versioning_and_resharding'] = rc
                }
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
                userRemoteConfigs: [[url: 'https://github.com/red-hat-storage/cephci.git']]
            ])
            script {
                sharedLib = load("${env.WORKSPACE}/pipeline/vars/common.groovy")
                sharedLib.prepareNode()
            }
        }
    }

    timeout(unit: "MINUTES", time: 120) {
        parallel testStages
    }

}
