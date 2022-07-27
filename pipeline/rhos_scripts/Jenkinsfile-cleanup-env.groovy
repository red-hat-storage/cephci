/*
    Pipeline script for clean up rhos-d environment.
*/
// Global variables section
def sharedLib

node("rhel-8-medium || ceph-qe-ci") {
    try {
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
            sharedLib = load("${env.WORKSPACE}/pipeline/vars/v3.groovy")
            sharedLib.prepareNode(2)
        }
        stage('cleanup') {
            println("Cleanup environment and Send Email to respective user")
            cmd = "${env.WORKSPACE}/.venv/bin/python ${env.WORKSPACE}/utility/psi_remove_vms.py --osp-cred ${env.HOME}/osp-cred-ci-2.yaml"
            println cmd

            rc = sh(script: "${cmd}", returnStatus: true)
            if (rc != 0) {
                println("Something went wrong... ${rc}")
                currentBuild.result = 'FAILURE'
            }
        }
    } catch(Exception err) {
        if (currentBuild.result == "ABORTED") {
            println("Aborting the workflow")
        }

        // notify about failure
        currentBuild.result = "FAILURE"
        def failureReason = err.getMessage()
        def subject =  "[CEPHCI-PIPELINE-ALERT] [JOB-FAILURE] - ${env.JOB_NAME}/${env.BUILD_NUMBER}"
        def body = "<body><h3><u>Job Failure</u></h3></p>"
        body += "<dl><dt>Jenkins Build:</dt><dd>${env.BUILD_URL}</dd>"
        body += "<dt>Failure Reason:</dt><dd>${failureReason}</dd></dl></body>"

        emailext (
            mimeType: 'text/html',
            subject: "${subject}",
            body: "${body}",
            from: "cephci@redhat.com",
            to: "cephci@redhat.com"
        )
        subject += "\n Jenkins URL: ${env.BUILD_URL}"
        googlechatnotification(url: "id:rhcephCIGChatRoom", message: subject)
    }
}
