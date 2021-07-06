/*
    Pipeline script for executing Tier 1 Cephfs test suites for RH Ceph 5.0.
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

            // prepare the node.
            sharedLib = load("${env.WORKSPACE}/pipeline/vars/common.groovy")
            sharedLib.prepareNode()
        }
    }

    timeout(unit: "MINUTES", time: 360) {
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

}
