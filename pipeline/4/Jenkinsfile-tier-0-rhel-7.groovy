/*
    Pipeline script for storing the latest build information of 4.x RHEL7 build.
*/
// Global variables section
def nodeName = "centos-7"
def cephVersion = "nautilus"
def sharedLib
def test_results = [:]

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

    stage('Publish Compose') {
        def composeInfo = sharedLib.fetchComposeInfo("${params.CI_MESSAGE}")
        def composeId = composeInfo.composeId

        // get rhbuild value from RHCEPH-4.3-RHEL-7.yyyymmdd.ci.x
        def rhcsVersion = composeId.substring(7,17).toLowerCase()

        withEnv(["rhcephVersion=${rhcsVersion}"]) {
            sharedLib.postLatestCompose(true)
        }
    }

}
