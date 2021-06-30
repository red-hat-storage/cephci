/*
    Pipeline script for executing Tier 0 test suites for RH Ceph 5.0.
*/
// Global variables section

def nodeName = "centos-7"
def cephVersion = "nautilus"
def sharedLib
def test_results = [:]
def rpmStages = ['deployRhel7': {
                    stage('RHEL7 RPM Deployment suite') {
                        script {
                            withEnv([
                                "osVersion=RHEL-7",
                                "sutVMConf=conf/inventory/rhel-7.9-server-x86_64.yaml",
                                "sutConf=conf/${cephVersion}/ansible/tier_0_deploy.yaml",
                                "testSuite=suites/${cephVersion}/ansible/tier_0_deploy_rpm_ceph.yaml",
                                "containerized=false",
                                "addnArgs=--post-results --log-level DEBUG",
                                "composeUrl=https://download.eng.bos.redhat.com/rhel-7/composes/auto/ceph-4.3-rhel-7/latest-RHCEPH-4-RHEL-7/"
                            ]) {
                                rc = sharedLib.runTestSuite()
                                test_results["deployRhel7"] = rc
                            }
                        }
                    }
                 }, 'deployRhel8': {
                    stage('RHEL8 RPM Deployment suite') {
                        script {
                            withEnv([
                                "osVersion=RHEL-8",
                                "sutVMConf=conf/inventory/rhel-8.4-server-x86_64.yaml",
                                "sutConf=conf/${cephVersion}/ansible/tier_0_deploy.yaml",
                                "testSuite=suites/${cephVersion}/ansible/tier_0_deploy_rpm_ceph.yaml",
                                "containerized=false",
                                "addnArgs=--post-results --log-level DEBUG"
                            ]) {
                                rc = sharedLib.runTestSuite()
                                test_results["deployRhel8"] = rc
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

    timeout(unit: "MINUTES", time: 120) {
        parallel rpmStages
    }


    stage('Publish Results') {
        script {
               sharedLib.sendEMail("Tier-0", test_results)
               if ( ! (1 in test_results.values()) ){
                   sharedLib.postLatestCompose()
                   sharedLib.sendUMBMessage("Tier0TestingDone")
               }
        }
    }

}
