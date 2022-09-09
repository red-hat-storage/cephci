/*
    Script to execute the cephci on baremetals
*/
// Global variables section
def sharedLib
def versions
def cephVersion
def composeUrl
def platform

// Pipeline script entry point
node("rhel-8-medium || ceph-qe-ci") {
        stage('prepareNode') {
            if (env.WORKSPACE) { sh script: "sudo rm -rf * .venv" }
            checkout(
                scm: [
                    $class: 'GitSCM',
                    branches: [[name: 'origin/master']],
                    extensions: [[
                        $class: 'CleanBeforeCheckout',
                        deleteUntrackedNestedRepositories: true
                    ], [
                        $class: 'WipeWorkspace'
                    ], [
                        $class: 'CloneOption',
                        depth: 1,
                        noTags: true,
                        shallow: true,
                        timeout: 10,
                        reference: ''
                    ]],
                    userRemoteConfigs: [[
                        url: 'https://github.com/red-hat-storage/cephci.git'
                    ]]
                   ],)

            sharedLib = load("${env.WORKSPACE}/pipeline/vars/v3.groovy")
            sharedLib.prepareNode(4)
        }
        }
