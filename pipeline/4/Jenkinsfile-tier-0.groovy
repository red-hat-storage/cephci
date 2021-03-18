/*
    Pipeline script for executing Tier 0 test suites for RH Ceph 4.x.
*/
// Global variables section
def nodeName = "centos-7"
def cephVersion = "nautilus"
def sharedLib
def testStages = ['sanity_rpm': {
                    stage('RPM Sanity suite') {
                        script {
                            withEnv([
                                "sutConf=conf/${cephVersion}/ansible/sanity-ceph-ansible.yaml",
                                "testSuite=suites/${cephVersion}/ansible/sanity_ceph_ansible.yaml",
                                "addnArgs=--post-results --log-level DEBUG"
                            ]) {
                                sharedLib.runTestSuite()
                            }
                        }
                    }
                 }, 'sanity_containerized': {
                    stage('Containerized Sanity suite') {
                        script {
                            withEnv([
                                "sutConf=conf/${cephVersion}/ansible/sanity-ceph-ansible.yaml",
                                "testSuite=suites/${cephVersion}/ansible/sanity_containerized_ceph_ansible.yaml",
                                "addnArgs=--post-results --log-level DEBUG"
                            ]) {
                                sharedLib.runTestSuite()
                            }
                        }
                    } 
                 }
                 ]

// Pipeline script entry point
node(nodeName) {

    timeout(unit: "MINUTES", time: 30) {
        stage('Install prereq') {
            checkout([
                $class: 'GitSCM',
                branches: [[name: '*/4.x_build']],
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
                userRemoteConfigs: [[url: 'https://github.com/manasagowri/cephci.git']]
            ])
            script {
                sharedLib = load("${env.WORKSPACE}/pipeline/vars/common.groovy")
                sharedLib.prepareNode()
            }
        }
    }

    timeout(unit: "HOURS", time: 12) {
        parallel testStages
    }

    stage('Publish Results') {
        script {
            sharedLib.sendEMail("Tier-0")
            sharedLib.postLatestCompose()
        }
    }
}

