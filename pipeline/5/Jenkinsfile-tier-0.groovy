/*
    Pipeline script for executing Tier 0 test suites for RH Ceph 5.0.
*/
// Global variables section

def nodeName = "centos-7"
def cephVersion = "pacific"
def sharedLib
def test_results = [:]
def testStages = ['cephadm': {
                    stage('Deployment suite') {
                        script {
                            withEnv([
                                "sutVMConf=conf/inventory/rhel-8.4-server-x86_64.yaml",
                                "sutConf=conf/${cephVersion}/cephadm/sanity-cephadm.yaml",
                                "testSuite=suites/${cephVersion}/cephadm/tier_0_cephadm.yaml",
                                "addnArgs=--post-results --log-level debug --grafana-image registry.redhat.io/rhceph-beta/rhceph-5-dashboard-rhel8:latest"
                            ]) {
                                rc = sharedLib.runTestSuite()
                                test_results["cephadm"] = rc
                            }
                        }
                    }
                 }, 'object': {
                    stage('Object suite') {
                        sleep(180)
                        script {
                            withEnv([
                                "sutVMConf=conf/inventory/rhel-8.4-server-x86_64-medlarge.yaml",
                                "sutConf=conf/${cephVersion}/rgw/tier_0_rgw.yaml",
                                "testSuite=suites/${cephVersion}/rgw/tier_0_rgw.yaml",
                                "addnArgs=--post-results --log-level debug"
                            ]) {
                                rc = sharedLib.runTestSuite()
                                test_results["object"] = rc
                            }
                        }
                    }
                 }, 'block': {
                    stage('Block suite') {
                        sleep(360)
                        script {
                            withEnv([
                                "sutVMConf=conf/inventory/rhel-8.4-server-x86_64-medlarge.yaml",
                                "sutConf=conf/${cephVersion}/rbd/tier_0_rbd.yaml",
                                "testSuite=suites/${cephVersion}/rbd/tier_0_rbd.yaml",
                                "addnArgs=--post-results --log-level debug"
                            ]) {
                                rc = sharedLib.runTestSuite()
                                test_results["block"] = rc
                            }
                        }
                    }
                 }, 'cephfs': {
                    stage('Cephfs Suite') {
                        sleep(480)
                        script {
                            withEnv([
                                "sutVMConf=conf/inventory/rhel-8.4-server-x86_64.yaml",
                                "sutConf=conf/${cephVersion}/cephfs/tier_0_fs.yaml",
                                "testSuite=suites/${cephVersion}/cephfs/tier_0_fs.yaml",
                                "addnArgs=--post-results --log-level debug"
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

    timeout(unit: "MINUTES", time: 120) {
        parallel testStages
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
