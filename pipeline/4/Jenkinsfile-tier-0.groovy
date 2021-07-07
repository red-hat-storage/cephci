/*
    Pipeline script for executing Tier 0 test suites for RH Ceph 4.x
*/
// Global variables section

def nodeName = "centos-7"
def cephVersion = "nautilus"
def sharedLib
def test_results = [:]
def defaultRHEL7BaseUrl
def defaultRHEL7Build
def rpmStages = ['deployRpmRhel7': {
                    stage('RHEL7 RPM') {
                        withEnv([
                            "osVersion=RHEL-7",
                            "sutVMConf=conf/inventory/rhel-7.9-server-x86_64.yaml",
                            "sutConf=conf/${cephVersion}/ansible/tier_0_deploy.yaml",
                            "testSuite=suites/${cephVersion}/ansible/tier_0_deploy_rpm_ceph.yaml",
                            "containerized=false",
                            "addnArgs=--post-results --log-level DEBUG",
                            "composeUrl=${defaultRHEL7BaseUrl}",
                            "rhcephVersion=${defaultRHEL7Build}"
                        ]) {
                            rc = sharedLib.runTestSuite()
                            test_results["deployRpmRhel7"] = rc
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
                                    "composeUrl=${defaultRHEL7BaseUrl}",
                                    "rhcephVersion=${defaultRHEL7Build}"
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
                                        "composeUrl=${defaultRHEL7BaseUrl}",
                                        "rhcephVersion=${defaultRHEL7Build}"
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
                userRemoteConfigs: [[
                    url: 'https://github.com/red-hat-storage/cephci.git'
                ]]
            ])
            script {
                sharedLib = load("${env.WORKSPACE}/pipeline/vars/common.groovy")
                sharedLib.prepareNode()
            }
        }
    }

    stage('Set RHEL7 vars') {
        // Gather the RHEL 7 latest compose information
        defaultRHEL7Build = sharedLib.getRHBuild("rhel-7")
        defaultRHEL7BaseUrl = sharedLib.getBaseUrl("rhel-7")
    }

    timeout(unit: "HOURS", time: 2) {
        parallel rpmStages
    }

    timeout(unit: "HOURS", time: 2) {
        parallel containerStages
    }

    timeout(unit: "HOURS", time: 2) {
        parallel functionalityStages
    }

    stage('Publish Results') {
        script {
               sharedLib.sendEMail("Tier-0", test_results)
               if ( ! (1 in test_results.values()) ){
                   sharedLib.postLatestCompose()
                   sharedLib.sendUMBMessage("Tier0TestingDone")

                   // RHEL7 Tier-0
                   def build = sharedLib.getRHBuild("rhel-7")
                   def rh7Tier0Json = "RHCEPH-${build}-tier0.json"

                   def rh7Compose = sharedLib.getPlatformComposeMap("rhel-7")
                   def composeInfo = [
                       "compose_id" : rh7Compose.compose_id,
                       "compose_url" : rh7Compose.compose_url,
                       "repository" : sharedLib.getRepository()
                   ]
                   def payload = writeJSON returnText: true, json: composeInfo

                   sharedLib.postCompose(payload, rh7Tier0Json)
               }
        }
    }

}
