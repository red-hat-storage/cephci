/*
    Pipeline script for executing Tier 1 object test suites for RH Ceph 5.0.
*/
// Global variables section

def nodeName = "centos-7"
def cephVersion = "pacific"
def sharedLib
def testResults = [:]

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
        stage('Single-site') {
            script {
                withEnv([
                    "sutVMConf=conf/inventory/rhel-8.4-server-x86_64.yaml",
                    "sutConf=conf/${cephVersion}/rgw/tier_1_rgw_cephadm.yaml",
                    "testSuite=suites/${cephVersion}/rgw/tier_1_object.yaml",
                    "addnArgs=--post-results --log-level DEBUG"
                ]) {
                    rc = sharedLib.runTestSuite()
                    testResults['Single_Site'] = rc
                }
            }
        }
    }

    timeout(unit: "MINUTES", time: 120) {
        stage('Multi-site') {
            script {
                withEnv([
                    "sutVMConf=conf/inventory/rhel-8.4-server-x86_64.yaml",
                    "sutConf=conf/${cephVersion}/rgw/rgw_mutlisite.yaml",
                    "testSuite=suites/${cephVersion}/rgw/rgw_multisite.yaml",
                    "addnArgs=--post-results --log-level DEBUG"
                ]) {
                    rc = sharedLib.runTestSuite()
                    testResults['Multi_Site_Buckets_tests'] = rc
                }
            }
        }
    }

    stage('Publish Results') {
        script {
            sharedLib.sendEMail("Object-Tier-1",testResults)
        }
    }
}
