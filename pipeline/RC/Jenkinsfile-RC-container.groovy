/*
    Pipeline script for storing the RC container information and triggering tier-0
    suites for RC.
*/
// Global variables section
def nodeName = "centos-7"
def sharedLib
def jsonContent

// Pipeline script entry point
node(nodeName) {

    timeout(unit: "MINUTES", time: 30) {
        stage("Install prereq") {
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

    stage("Publish RC Container Compose") {
        jsonContent = sharedLib.postRCCompose("container")
        if (!jsonContent) {
            sh "exit 1"
        }
    }

    stage("Execute Tier-0 suite") {
        def composeMap = readJSON text: "${jsonContent}"
        def rhcephMajorVersion = composeMap.compose_id.substring(7,8)
        def jobName = "rhceph-${rhcephMajorVersion}-tier-0"

        build ([
            wait: false,
            job: jobName,
            parameters: [string(name: 'CI_MESSAGE', value: jsonContent)]
        ])
    }

}
