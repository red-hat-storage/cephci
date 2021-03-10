/*
    Pipeline script for executing Tier 0 test suites for RH Ceph 5.0.
*/
// Global variables section

def nodeName = "centos-7"
def cephVersion = "pacific"
def sharedLib
def testStages = ['deploy': {
                    stage('Deployment suite') {
                        script {
                            withEnv([
                                "sutVMConf=conf/inventory/rhel-8.3-server-x86_64.yaml",
                                "sutConf=conf/${cephVersion}/cephadm/sanity-cephadm.yaml",
                                "testSuite=suites/${cephVersion}/cephadm/cephadm_deploy.yaml",
                                "addnArgs=--post-results --log-level DEBUG"
                            ]) {
                                sharedLib.runTestSuite()
                            }
                        }
                    }
                 }, 'upgrade': {

                    stage('Upgrade suite') {
                        println "Work in progress"
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

