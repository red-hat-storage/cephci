/*
    Script that updates upstream recipe file 
*/
// Global variables section
def sharedLib
def upstreamVersion = "${params.releaseName}" ?: "" // Gets the upstream version to be executed from params
def osType = "centos" // OS type
def osVersion = "9"   // OS Version
currentBuild.description = "Upstream-branch: ${upstreamVersion}  Distro: ${osType}-${osVersion}"

// Pipeline script entry point
node("rhel-9-medium") {
    try {
        timeout(unit: "MINUTES", time: 30) {
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
                    ],
                    changelog: false,
                    poll: false
                )
                sharedLib = load("${env.WORKSPACE}/pipeline/vars/v3.groovy")
                sharedLib.prepareNode()
            }

            stage('updateRecipeFile') {
                try{
                    sharedLib.updateUpstreamFile(upstreamVersion, osType, osVersion) //Updates upstream.yaml
                } catch(Exception err) {
                    retry(10) {
                        echo "Execution failed, Retrying..."
                        sharedLib.updateUpstreamFile(upstreamVersion, osType, osVersion)
                    }
                    currentBuild.result = "ABORTED"
                    println err.getMessage()
                    error("Encountered an error")
                }
            }
        }
    } catch(Exception err) {
        if (currentBuild.result != "ABORTED") {
            // notify about failure
            currentBuild.result = "FAILURE"
            def failureReason = err.getMessage()
            def subject =  "[CEPHCI-PIPELINE-ALERT] [JOB-FAILURE] - ${env.JOB_NAME}/${env.BUILD_NUMBER}"
            def body = "<body><h3><u>Job Failure</u></h3></p>"
            body += "<dl><dt>Jenkins Build:</dt><dd>${env.BUILD_URL}</dd>"
            body += "<dt>Failure Reason:</dt><dd>${failureReason}</dd>"
            body += "<dt>Failure Snippet:</dt><dd>${sharedLib.returnSnippet()}</dd></dl></body>"

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
    } finally {
        if ( upstreamVersion == "main" ) {
            build ([
                wait: false,
                job: "rhceph-upstream-listener",
                parameters: [string(name: 'releaseName', value: "quincy")]
            ])
        }
    }
}
