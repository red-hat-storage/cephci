/*
    Pipeline script for executing Tier 1 Cephfs test suites for RH Ceph 4.x
*/
// Global variables section

def nodeName = "centos-7"
def cephVersion = "nautilus"
def sharedLib

// Pipeline script entry point

node(nodeName) {

    timeout(unit: "MINUTES", time: 30) {
        stage('Install Prereq') {
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

    stage('Cephfs Extended MAT Suite') {
        withEnv([
            "sutVMConf=conf/inventory/rhel-8.4-server-x86_64-medlarge.yaml",
            "sutConf=conf/${cephVersion}/cephfs/tier_1_fs.yaml",
            "testSuite=suites/${cephVersion}/cephfs/tier_1_fs.yaml",
            "addnArgs=--post-results --log-level info"
        ]) {
            sharedLib.runTestSuite()
        }
    }
}
