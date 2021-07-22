/*
    Pipeline script for executing Tier 1 object test suites for RH Ceph 4.x
*/
// Global variables section

def nodeName = "centos-7"
def cephVersion = "nautilus"
def sharedLib

// Pipeline script entry point

node(nodeName) {

    timeout(unit: "MINUTES", time: 30) {
        stage('Install prereq') {
            checkout([
                $class: 'GitSCM',
                branches: [[name: '*/wrapper_4x']],
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
                    url: 'https://github.com/udaysk23/cephci.git'
                ]]
            ])

            sharedLib = load("${env.WORKSPACE}/pipeline/vars/common.groovy")
            sharedLib.prepareNode()
        }
    }

    stage('Single-site') {
        withEnv([
            "sutVMConf=conf/inventory/rhel-7.9-server-x86_64.yaml",
            "sutConf=conf/${cephVersion}/rgw/tier_1_rgw.yaml",
            "testSuite=suites/${cephVersion}/rgw/tier_1_object.yaml",
            "addnArgs=--post-results --log-level DEBUG"
        ]) {
            sharedLib.runTestSuite()
        }
    }
    stage('Multi-site') {
        withEnv([
            "sutVMConf=conf/inventory/rhel-7.9-server-x86_64.yaml",
            "sutConf=conf/${cephVersion}/rgw/tier_1_rgw_multisite.yaml",
            "testSuite=suites/${cephVersion}/rgw/tier_1_rgw_multisite.yaml",
            "addnArgs=--post-results --log-level DEBUG"
        ]) {
            sharedLib.runTestSuite()
        }
    }

}
