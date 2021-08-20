/*
    Pipeline script for executing Tier 2 object test suites for RH Ceph 5.0.
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

            // Prepare the node for executing Ceph QE Test suites
            sharedLib = load("${env.WORKSPACE}/pipeline/vars/common.groovy")
            sharedLib.prepareNode()
        }
    }

    stage('Regression') {
        withEnv([
            "sutVMConf=conf/inventory/rhel-8.4-server-x86_64-medlarge.yaml",
            "sutConf=conf/${cephVersion}/rgw/tier_2_rgw_regression.yaml",
            "testSuite=suites/${cephVersion}/rgw/tier_2_rgw_regression.yaml",
            "addnArgs=--post-results --log-level INFO"
        ]) {
            sharedLib.runTestSuite()
        }
    }

    stage('Secure S3Tests') {
        withEnv([
            "sutVMConf=conf/inventory/rhel-8.4-server-x86_64-medlarge.yaml",
            "sutConf=conf/${cephVersion}/rgw/tier_0_rgw.yaml",
            "testSuite=suites/${cephVersion}/rgw/tier_2_rgw_ssl_s3tests.yaml",
            "addnArgs=--post-results --log-level debug"
        ]) {
            sharedLib.runTestSuite()
        }
    }

}
