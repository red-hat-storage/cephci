/*
    Pipeline script for executing Tier 0 test suites for RH Ceph 5.0.
*/
// Global variables section

def nodeName = "centos-7"
def cephVersion = "nautilus"
def sharedLib
def test_results = [:]
def rpmStages = ['deployRpmRhel7': {
                    stage('RHEL7 RPM') {
                        script {
                            withEnv([
                                "osVersion=RHEL-7",
                                "sutVMConf=conf/inventory/rhel-7.9-server-x86_64.yaml",
                                "sutConf=conf/${cephVersion}/ansible/tier_0_deploy.yaml",
                                "testSuite=suites/${cephVersion}/ansible/tier_0_deploy_rpm_ceph.yaml",
                                "containerized=false",
                                "addnArgs=--post-results --log-level DEBUG",
                                "composeUrl=http://download.eng.bos.redhat.com/rhel-7/composes/auto/ceph-4.3-rhel-7/latest-RHCEPH-4-RHEL-7/"
                            ]) {
                                rc = sharedLib.runTestSuite()
                                test_results["deployRpmRhel7"] = rc
                            }
                        }
                    }
                 },
                 'deployRpmRhel8': {
                    stage('RHEL8 RPM') {
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
                                test_results["deployRpmRhel8"] = rc
                            }
                        }
                    }
                 }]


def containerStages = ['deployContainerRhel7': {
                        stage('RHEL7 Container') {
                            script {
                                withEnv([
                                    "osVersion=RHEL-7",
                                    "sutVMConf=conf/inventory/rhel-7.9-server-x86_64.yaml",
                                    "sutConf=conf/${cephVersion}/ansible/tier_0_deploy.yaml",
                                    "testSuite=suites/${cephVersion}/ansible/tier_0_deploy_containerized_ceph.yaml",
                                    "addnArgs=--post-results --log-level DEBUG",
                                    "composeUrl=http://download.eng.bos.redhat.com/rhel-7/composes/auto/ceph-4.3-rhel-7/latest-RHCEPH-4-RHEL-7/"
                                ]) {
                                    rc = sharedLib.runTestSuite()
                                    test_results["deployContainerRhel7"] = rc
                                }
                            }
                        }
                    },
                    'deployContainerRhel8': {
                        stage('RHEL8 Container') {
                            script {
                                withEnv([
                                    "osVersion=RHEL-8",
                                    "sutVMConf=conf/inventory/rhel-8.4-server-x86_64.yaml",
                                    "sutConf=conf/${cephVersion}/ansible/tier_0_deploy.yaml",
                                    "testSuite=suites/${cephVersion}/ansible/tier_0_deploy_containerized_ceph.yaml",
                                    "addnArgs=--post-results --log-level DEBUG"
                                ]) {
                                    rc = sharedLib.runTestSuite()
                                    test_results["deployContainerRhel8"] = rc
                                }
                            }
                        }
                    }]


def functionalityStages = [ 'object': {
                            stage('Object suite') {
                                script {
                                    withEnv([
                                        "osVersion=RHEL-8",
                                        "sutVMConf=conf/inventory/rhel-8.4-server-x86_64-medlarge.yaml",
                                        "sutConf=conf/${cephVersion}/rgw/tier_0_rgw.yaml",
                                        "testSuite=suites/${cephVersion}/rgw/tier_0_rgw.yaml",
                                        "containerized=false",
                                        "addnArgs=--post-results --log-level DEBUG"
                                    ]) {
                                        rc = sharedLib.runTestSuite()
                                        test_results["object"] = rc
                                    }
                                }
                            }
                        },
                        'block': {
                            stage('Block suite') {
                                script {
                                    withEnv([
                                        "osVersion=RHEL-8",
                                        "sutVMConf=conf/inventory/rhel-8.4-server-x86_64-medlarge.yaml",
                                        "sutConf=conf/${cephVersion}/rbd/tier_0_rbd.yaml",
                                        "testSuite=suites/${cephVersion}/rbd/tier_0_rbd.yaml",
                                        "containerized=false",
                                        "addnArgs=--post-results --log-level DEBUG"
                                    ]) {
                                        rc = sharedLib.runTestSuite()
                                        test_results["block"] = rc
                                    }
                                }
                            }
                        },
                        'cephfs': {
                            stage('Cephfs Suite') {
                                script {
                                    withEnv([
                                        "osVersion=RHEL-7",
                                        "sutVMConf=conf/inventory/rhel-7.9-server-x86_64.yaml",
                                        "sutConf=conf/${cephVersion}/cephfs/tier_0_fs.yaml",
                                        "testSuite=suites/${cephVersion}/cephfs/tier_0_fs.yaml",
                                        "containerized=false",
                                        "addnArgs=--post-results --log-level debug",
                                        "composeUrl=http://download.eng.bos.redhat.com/rhel-7/composes/auto/ceph-4.3-rhel-7/latest-RHCEPH-4-RHEL-7/"
                                    ]) {
                                        rc = sharedLib.runTestSuite()
                                        test_results["cephfs"] = rc
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

    timeout(unit: "MINUTES", time: 70) {
        parallel rpmStages
    }

    timeout(unit: "MINUTES", time: 70) {
        parallel containerStages
    }

    timeout(unit: "MINUTES", time: 100) {
        parallel functionalityStages
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
