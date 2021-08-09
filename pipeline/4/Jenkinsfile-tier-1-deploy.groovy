/*
    Pipeline script for executing Tier 1 Deploy test suites for RH Ceph 4.x
*/
// Global variables section

def nodeName = "centos-7"
def cephVersion = "nautilus"
def sharedLib
def defaultRHEL7BaseUrl
def defaultRHEL7Build

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

    stage('Set RHEL7 vars') {
    // Gather the RHEL 7 latest compose information
        defaultRHEL7Build = sharedLib.getRHBuild("rhel-7")
        defaultRHEL7BaseUrl = sharedLib.getBaseUrl("rhel-7", "tier1")}

    stage('Deploy-RPM') {
        withEnv([
            "sutVMConf=conf/inventory/rhel-7.9-server-x86_64.yaml",
            "sutConf=conf/${cephVersion}/ansible/tier_1_deploy.yaml",
            "testSuite=suites/${cephVersion}/ansible/tier_1_deploy.yaml",
            "addnArgs=--post-results --log-level DEBUG",
            "composeUrl=${defaultRHEL7BaseUrl}",
            "rhcephVersion=${defaultRHEL7Build}"
        ]) {
            sharedLib.runTestSuite()
        }
    }

    stage('Upgrade-RPM') {
        withEnv([
            "sutVMConf=conf/inventory/rhel-7.9-server-x86_64.yaml",
            "sutConf=conf/${cephVersion}/upgrades/tier_1_upgrade.yaml",
            "testSuite=suites/${cephVersion}/upgrades/tier_1_upgrade_rpm.yaml",
            "addnArgs=--post-results --log-level DEBUG",
            "composeUrl=${defaultRHEL7BaseUrl}",
            "rhcephVersion=${defaultRHEL7Build}"
        ]) {
            sharedLib.runTestSuite()
        }
    }

    stage('Upgrade-Container') {
        withEnv([
            "sutVMConf=conf/inventory/rhel-8.4-server-x86_64.yaml",
            "sutConf=conf/${cephVersion}/upgrades/tier_1_upgrade.yaml",
            "testSuite=suites/${cephVersion}/upgrades/tier_1_upgrade_container.yaml",
            "addnArgs=--post-results --log-level DEBUG",
        ]) {
            sharedLib.runTestSuite()
        }
    }
}
