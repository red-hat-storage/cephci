/*
    Pipeline script for executing Tier 1 object test suites for RH Ceph 5.x.
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

    stage('Single-site') {
        withEnv([
            "sutVMConf=conf/inventory/rhel-8.4-server-x86_64.yaml",
            "sutConf=conf/${cephVersion}/rgw/tier_1_rgw_cephadm.yaml",
            "testSuite=suites/${cephVersion}/rgw/tier_1_object.yaml",
            "addnArgs=--post-results --log-level DEBUG"
        ]) {
            sharedLib.runTestSuite()
        }
    }

   stage('Multi-site') {
        withEnv([
            "sutVMConf=conf/inventory/rhel-8.4-server-x86_64.yaml",
            "sutConf=conf/${cephVersion}/rgw/rgw_mutlisite.yaml",
            "testSuite=suites/${cephVersion}/rgw/rgw_multisite.yaml",
            "addnArgs=--post-results --log-level DEBUG"
        ]) {
            sharedLib.runTestSuite()
        }
    }

}
