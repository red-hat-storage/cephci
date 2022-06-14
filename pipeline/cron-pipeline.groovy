/*
    Pipeline script for triggering scheduled cron job.
*/

def nodeName = "centos-7"
def sharedLib
def buildArtifacts
def release


node(nodeName) {
    try {
        timeout(unit: "MINUTES", time: 30) {
            stage('Install prereq') {
                if (env.WORKSPACE) { sh script: "sudo rm -rf * .venv" }
                checkout(
                    scm: [
                        $class: 'GitSCM',
                        branches: [[name: 'origin/master']],
                        extensions: [
                            [
                                $class: 'CleanBeforeCheckout',
                                deleteUntrackedNestedRepositories: true
                            ],
                            [
                                $class: 'WipeWorkspace'
                            ],
                            [
                                $class: 'CloneOption',
                                depth: 1,
                                noTags: true,
                                shallow: true,
                                timeout: 10,
                                reference: ''
                            ]
                        ],
                        userRemoteConfigs: [[
                            url: 'https://github.com/red-hat-storage/cephci.git'
                        ]]
                    ],
                    changelog: false,
                    poll: false
                )

                // prepare the node
                sharedLib = load("${env.WORKSPACE}/pipeline/vars/lib.groovy")
                sharedLib.prepareNode()
            }
        }

        stage('PrepareStage') {
            def defaultFileDir = "/ceph/cephci-jenkins/latest-rhceph-container-info"
            def updatedFiles = sh (
                returnStdout: true,
                script: "find ${defaultFileDir} -name RHCEPH-*.yaml -mtime -7 -ls"
            )
            if (!updatedFiles) {
                currentBuild.result = "ABORTED"
                error "Recipe file updates are not available."
            }
            def filePaths = updatedFiles.split("\\n")
            for (filePath in filePaths) {
                def fileName = filePath.tokenize("/")[-1]
                def latestContent = sharedLib.yamlToMap(fileName, defaultFileDir)
                if (latestContent.containsKey('latest')) {
                    def latestBuild = latestContent.latest
                    println "latestBuild : ${latestBuild}"
                    buildArtifacts = writeJSON returnText: true, json: latestBuild
                    release = fileName.split(".yaml")[0]
                    println "Release : ${release}"
                    build ([
                        wait: false,
                        job: "rhceph-cron-pipeline-test-executor",
                        parameters: [string(name: 'rhcephVersion', value: release),
                                    string(name: 'buildType', value: 'stage-0'),
                                    string(name: 'buildArtifacts', value: buildArtifacts)]
                    ])
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
}
