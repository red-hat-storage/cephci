/*
    Pipeline script for executing Tier 1 Bootstrap test suites for RH Ceph 5.0.
*/
// Global variables section

def nodeName = "centos-7"
def cephVersion = "pacific"
def sharedLib
def testStages = ['Bootstrap_custom_SSL_dashboard_port': {
                    stage('Bootstrap suite - Custom SSL dashboard port') {
                        script {
                            withEnv([
                                "sutVMConf=conf/inventory/rhel-8.3-server-x86_64.yaml",
                                "sutConf=conf/${cephVersion}/cephadm/tier1_3node_cephadm_bootstrap.yaml",
                                "testSuite=suites/${cephVersion}/cephadm/tier1_ssl_dashboard_port.yaml",
                                "addnArgs=--post-results --log-level DEBUG"
                            ]) {
                                sharedLib.runTestSuite()
                            }
                        }
                    }
                 }, 'Bootstrap_skip_dashboard': {
                    stage('Bootstrap suite - Skip dashboard') {
                        sleep(180)
                        script {
                            withEnv([
                                "sutVMConf=conf/inventory/rhel-8.3-server-x86_64.yaml",
                                "sutConf=conf/${cephVersion}/cephadm/tier1_3node_cephadm_bootstrap.yaml",
                                "testSuite=suites/${cephVersion}/cephadm/tier1_skip_dashboard.yaml",
                                "addnArgs=--post-results --log-level debug"
                            ]) {
                                sharedLib.runTestSuite()
                            }
                        }
                    }
                 }, 'Bootstrap_skip_monitoring_stack_orphan_initial_daemons': {
                    stage('Bootstrap suite - skip-monitoring-stack and orphan-initial-daemons') {
                        sleep(360)
                        script {
                            withEnv([
                                "sutVMConf=conf/inventory/rhel-8.3-server-x86_64.yaml",
                                "sutConf=conf/${cephVersion}/cephadm/tier1_3node_cephadm_bootstrap.yaml",
                                "testSuite=suites/${cephVersion}/cephadm/tier1_cephadm_bootstrap.yaml",
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

    timeout(unit: "MINUTES", time: 120) {
        parallel testStages
    }

    stage('Publish Results') {
        script {
            sharedLib.sendEMail("Tier-1")
            sharedLib.postLatestCompose()
        }
    }

}
