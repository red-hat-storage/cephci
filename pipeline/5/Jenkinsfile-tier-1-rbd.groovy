    /*
    Pipeline script for executing Tier 1 RBD test suites for RH Ceph 5.0.
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

    timeout(unit: "MINUTES", time: 360) {
        stage('RBD Extended Suite') {
            script {
                withEnv([
                    "sutVMConf=conf/inventory/rhel-8.4-server-x86_64-medlarge.yaml",
                    "sutConf=conf/${cephVersion}/rbd/tier_0_rbd.yaml",
                    "testSuite=suites/${cephVersion}/rbd/tier_1_rbd.yaml",
                    "addnArgs=--post-results --log-level info"
                ]) {
                    rc = sharedLib.runTestSuite()
                    testResults['Extended test'] = rc
                }
            }
        }
    }

    timeout(unit: "MINUTES", time: 480) {
        stage('RBD Mirror Suite') {
            script {
                withEnv([
                    "sutVMConf=conf/inventory/rhel-8.4-server-x86_64-medlarge.yaml",
                    "sutConf=conf/${cephVersion}/rbd/tier_1_rbd_mirror.yaml",
                    "testSuite=suites/${cephVersion}/rbd/tier_1_rbd_mirror.yaml",
                    "addnArgs=--post-results --log-level info"
                ]) {
                    rc = sharedLib.runTestSuite()
                    testResults['RBD Mirror test'] = rc
                }
            }
        }
    }

    stage('Publish Results') {
        script {
            sharedLib.sendEMail("RBD-Tier-1", testResults)
        }
    }

}
