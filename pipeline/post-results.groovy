// Pipeline script for uploading IBM test run results to report portal.
def credsRpProc = [:]
def sharedLib
def rpPreprocDir
def tierLevel = null
def stageLevel = null
def run_type = "Sanity Run"
def build_url
def reportBucket = "qe-ci-reports"
def remoteName= "ibm-cos"
def msgMap = [:]

node("rhel-8-medium || ceph-qe-ci") {

    try {

        stage('prepareJenkinsAgent') {
            if (env.WORKSPACE) { sh script: "sudo rm -rf * .venv" }
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

            // prepare the node
            sharedLib = load("${env.WORKSPACE}/pipeline/vars/v3.groovy")
            msgMap = sharedLib.getCIMessageMap()
            println("msgMap : ${msgMap}")
            println("sharedLib: ${sharedLib}")
            sharedLib.prepareNode(3)
        }

        stage('configureReportPortalWorkDir') {
            (rpPreprocDir, credsRpProc) = sharedLib.configureRpPreProc()
        }

        stage('uploadTestResult') {
            msgMap = sharedLib.getCIMessageMap()
            def composeInfo = msgMap["recipe"]

            def resultDir = msgMap["test"]["object-prefix"]
            println("Test results are available at ${resultDir}")

            def tmpDir = sh(returnStdout: true, script: "mktemp -d").trim()
            sh script: "rclone sync ${remoteName}://${reportBucket} ${tmpDir} --progress --create-empty-src-dirs"

            def metaData = readYaml file: "${tmpDir}/${resultDir}/metadata.yaml"
            def copyFiles = "cp -a ${tmpDir}/${resultDir}/results ${rpPreprocDir}/payload/"
            def copyAttachments = "cp -a ${tmpDir}/${resultDir}/attachments ${rpPreprocDir}/payload/"
            def rmTmpDir = "rm -rf ${tmpDir}"

            // Modifications to reuse methods
            metaData["ceph_version"] = metaData["ceph-version"]
            if ( metaData["stage"] == "latest" ) { metaData["stage"] = "Tier-0" }

            sh script: "${copyFiles} && ${copyAttachments} && ${rmTmpDir}"

            if ( composeInfo ){
                metaData["buildArtifacts"] = composeInfo
            }

            def testStatus = "ABORTED"
            /* Publish results through E-mail and Google Chat */
            if ( msgMap["pipeline"].containsKey("tags") ) {
                def tag = msgMap["pipeline"]["tags"]
                def tags_list = tag.split(',') as List
                def stage_index = tags_list.findIndexOf { it ==~ /stage-\w+/ }
                stageLevel = tags_list.get(stage_index)
                def tier_index = tags_list.findIndexOf { it ==~ /tier-\w+/ }
                tierLevel = tags_list.get(tier_index)
                run_type = msgMap["pipeline"]["run_type"]
                build_url = msgMap["run"]["url"]
            }

            if (metaData["results"]) {
                if ( ! msgMap["pipeline"].containsKey("tags") ) {
                    sharedLib.sendEmail(
                        metaData["results"], metaData, metaData["stage"]
                    )
                }
                sharedLib.uploadTestResults(rpPreprocDir, credsRpProc, metaData)

            }
            testStatus = msgMap["test"]["result"]

            // Remove the sync results folder
            sh script: "rclone purge ${remoteName}:${reportBucket}/${resultDir}"

            // Update RH recipe file
            if ( composeInfo != null ) {
                if ( run_type == "Sanity Run") {
                    if ( tierLevel == null ) {
                        tierLevel = msgMap["pipeline"]["name"]
                    }
                    def rhcsVersion = sharedLib.getRHCSVersionFromArtifactsNvr()
                    majorVersion = rhcsVersion["major_version"]
                    minorVersion = rhcsVersion["minor_version"]
                    minorVersion = "${minorVersion}"

                    def latestContent = sharedLib.readFromReleaseFile(
                        majorVersion, minorVersion
                    )
                    println("latestContentBefore: ${latestContent}")

                    if ( latestContent.containsKey(tierLevel) ) {
                        latestContent[tierLevel] = composeInfo
                    }
                    else {
                        def updateContent = ["${tierLevel}": composeInfo]
                        latestContent += updateContent
                    }
                    println("latestContent: ${latestContent}")
                    sharedLib.writeToReleaseFile(
                        majorVersion, minorVersion, latestContent
                    )
                }
                sharedLib.writeToResultsFile(
                    msgMap["artifact"]["version"],
                    tierLevel,
                    stageLevel,
                    testStatus,
                    run_type,
                    build_url
                )
            }
            if (msgMap["pipeline"]["final_stage"] && tierLevel == "tier-2") {
                def rhcsVersion = sharedLib.getRHCSVersionFromArtifactsNvr()
                majorVersion = rhcsVersion["major_version"]
                minorVersion = rhcsVersion["minor_version"]
                minorVersion = "${minorVersion}"

                def testResults = sharedLib.readFromResultsFile(
                    msgMap["artifact"]["version"]
                )
                sharedLib.updateConfluencePage(
                    majorVersion,
                    minorVersion,
                    msgMap["artifact"]["version"],
                    run_type,
                    testResults
                )
                sharedLib.sendConsolidatedEmail(
                    run_type,
                    testResults,
                    metaData,
                    majorVersion,
                    minorVersion,
                    msgMap["artifact"]["version"]
                )
                sharedLib.sendGChatNotification(
                    run_type, metaData["results"], tierLevel, stageLevel, build_url
                )

                // Remove the release file as it wouldn't be required
            }
            println("Execution complete")
        }
    } catch(Exception err) {
        if (currentBuild.result == "ABORTED") {
            println("The workflow has been aborted.")
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
