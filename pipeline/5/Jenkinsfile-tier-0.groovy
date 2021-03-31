/*
    Pipeline script for executing Tier 0 test suites for RH Ceph 5.0.
*/
// Global variables section

def nodeName = "centos-7"
def cephVersion = "pacific"
def sharedLib
def testStages = ['cephadm': {
                    stage('Deployment suite') {
                        script {
                            withEnv([
                                "sutVMConf=conf/inventory/rhel-8.3-server-x86_64.yaml",
                                "sutConf=conf/${cephVersion}/cephadm/sanity-cephadm.yaml",
                                "testSuite=suites/${cephVersion}/cephadm/tier_0_cephadm.yaml",
                                "addnArgs=--post-results --log-level DEBUG"
                            ]) {
                                sharedLib.runTestSuite()
                            }
                        }
                    }
                 }, 'object': {
                    stage('Object suite') {
                        sleep(10)
                        script {
                            withEnv([
                                "sutVMConf=conf/inventory/rhel-8.3-server-x86_64-medlarge.yaml",
                                "sutConf=conf/${cephVersion}/rgw/sanity_rgw.yaml",
                                "testSuite=suites/${cephVersion}/rgw/tier_0_rgw.yaml",
                                "addnArgs=--post-results --log-level debug"
                            ]) {
                                sharedLib.runTestSuite()
                            }
                        }
                    }
                 }]

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

    timeout(unit: "MINUTES", time: 60) {
        parallel testStages
    }

    stage('Publish Results') {
        script {
            sharedLib.sendEMail("Tier-0")
            sharedLib.postLatestCompose()
        }
    }

}

