/*
    Pipeline script for executing Tier 1 RBD test suites for RH Ceph 5.x
*/
// Global variables section

def nodeName = "centos-7"
def cephVersion = "pacific"
def sharedLib

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

    timeout(unit: "HOURS", time: 2) {
        stage('Extended MAT') {
            withEnv([
                "sutVMConf=conf/inventory/rhel-8.4-server-x86_64-medlarge.yaml",
                "sutConf=conf/${cephVersion}/rbd/tier_0_rbd.yaml",
                "testSuite=suites/${cephVersion}/rbd/tier_1_rbd.yaml",
                "addnArgs=--post-results --log-level info"
            ]) {
                sharedLib.runTestSuite()
            }
        }
    }

    timeout(unit: "HOURS", time: 2) {
        stage('RBD Mirror') {
            withEnv([
                "sutVMConf=conf/inventory/rhel-8.4-server-x86_64-medlarge.yaml",
                "sutConf=conf/${cephVersion}/rbd/tier_1_rbd_mirror.yaml",
                "testSuite=suites/${cephVersion}/rbd/tier_1_rbd_mirror.yaml",
                "addnArgs=--post-results --log-level info"
            ]) {
                sharedLib.runTestSuite()
            }
        }
    }

}
