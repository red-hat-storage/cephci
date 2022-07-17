/*
    Pipeline script for clean up of orphaned volumes in rhos-d environment.
*/
// Global variables section
def sharedLib

node("rhel-8-medium") {

    stage('prepareNode') {
        checkout(
                scm: [
                    $class: 'GitSCM',
                    branches: [[name: "origin/master"]],
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
                        url: "https://github.com/red-hat-storage/cephci.git"
                    ]]
                ],
                changelog: false,
                poll: false
            )
        sharedLib = load("${env.WORKSPACE}/pipeline/vars/common.groovy")
        sharedLib.prepareNode()
    }

    stage('cleanUp') {
        println("Cleanup Orphaned cloud volumes  and Send Email to respective user")
        cmd = "${env.WORKSPACE}/.venv/bin/python"
        cmd += " ${env.WORKSPACE}/utility/psi_volume_cleanup.py"
        cmd += " --osp-cred ${env.HOME}/osp-cred-ci-2.yaml"
        println(cmd)

        rc = sh(script: "${cmd}", returnStatus: true)
        if (rc != 0) {
            println("Something went wrong... ${rc}")
            currentBuild.result = 'FAILURE'
        }
    }
}
