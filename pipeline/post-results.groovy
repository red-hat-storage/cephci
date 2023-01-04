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
def tags_list
def umbLib
def composeInfo
def metaData = [:]
def rp_base_link = "https://reportportal-rhcephqe.apps.ocp-c1.prod.psi.redhat.com"
def launch_id = ""

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
            sharedLib.prepareNode(3)

            msgMap = sharedLib.getCIMessageMap()
            println("msgMap : ${msgMap}")
            println("sharedLib: ${sharedLib}")

            emailLib = load("${env.WORKSPACE}/pipeline/vars/email.groovy")
            println("emailLib : ${emailLib}")
            reportLib = load("${env.WORKSPACE}/pipeline/vars/upload_results.groovy")
            println("reportLib : ${reportLib}")
        }

        stage('updatePipelineMetadata'){
            println("Stage updatePipelineMetadata")
            if ( msgMap["pipeline"].containsKey("tags") ) {
                def tag = msgMap["pipeline"]["tags"]
                tags_list = tag.split(',') as List
                def stage_index = tags_list.findIndexOf { it ==~ /stage-\w+/ }
                stageLevel = tags_list.get(stage_index)
                def tier_index = tags_list.findIndexOf { it ==~ /tier-\w+/ }
                tierLevel = tags_list.get(tier_index)
                run_type = msgMap["pipeline"]["run_type"]
                build_url = msgMap["run"]["url"]
            }
        }

        stage('configureReportPortalWorkDir') {
            println("Stage configureReportPortalWorkDir")
            (rpPreprocDir, credsRpProc) = reportLib.configureRpPreProc(sharedLib)
        }

        stage('uploadTestResultToReportPortal'){
            println("Stage uploadTestResultToReportPortal")
            composeInfo = msgMap["recipe"]

            def resultDir = msgMap["test"]["object-prefix"]
            println("Test results are available at ${resultDir}")

            def tmpDir = sh(returnStdout: true, script: "mktemp -d").trim()
            tags_list = msgMap["pipeline"]["tags"].split(',') as List
            if ('ibmc' in tags_list) {
                sh script: "rclone sync ${remoteName}://${reportBucket} ${tmpDir} --progress --create-empty-src-dirs"
            } else {
                sh "sudo cp -r /ceph/cephci-jenkins/${resultDir} ${tmpDir}"
            }

            metaData = readYaml file: "${tmpDir}/${resultDir}/metadata.yaml"
            println("metadata: ${metaData}")
            def copyFiles = "sudo cp -a ${tmpDir}/${resultDir}/results ${rpPreprocDir}/payload/"
            def copyAttachments = "sudo cp -a ${tmpDir}/${resultDir}/attachments ${rpPreprocDir}/payload/"
            def rmTmpDir = "sudo rm -rf ${tmpDir}"

            // Modifications to reuse methods
            metaData["ceph_version"] = metaData["ceph-version"]
            if ( metaData["stage"] == "latest" ) { metaData["stage"] = "Tier-0" }

            sh script: "${copyFiles} && ${copyAttachments} && ${rmTmpDir}"

            if ( composeInfo ){
                metaData["buildArtifacts"] = composeInfo
            }

            if (metaData["results"]) {
                launch_id = reportLib.uploadTestResults(rpPreprocDir, credsRpProc, metaData, stageLevel, run_type)
                println("launch_id: ${launch_id}")
                if (launch_id) {
                    metaData["rp_link"] = "${rp_base_link}/ui/#cephci/launches/all/${launch_id}"
                }
            }

            // Remove the sync results folder
            if ('ibmc' in tags_list) {
                sh script: "rclone purge ${remoteName}:${reportBucket}/${resultDir}"
            } else {
                sh "sudo rm -r /ceph/cephci-jenkins/${resultDir}"
            }
        }

        stage('notifyTier-0Failure'){
            println("Stage notifyTier-0Failure")
            if (msgMap["test"]["result"] == "FAILURE" && tierLevel == "tier-0") {
                metaData.put("version", msgMap["artifact"]["nvr"])
                println("version")
                println(msgMap["artifact"]["nvr"])
                emailLib.sendEmail(
                    sharedLib,
                    run_type,
                    metaData['results'],
                    metaData,
                    tierLevel,
                    stageLevel,
                    msgMap["artifact"]["nvr"]
                )

                emailLib.sendGChatNotification(
                    run_type, metaData["results"], tierLevel, stageLevel, build_url
                )
            }
        }

        stage('updateRecipeFile'){
            println("Stage updateRecipeFile")
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
            }
        }

        stage('updateResultsFile'){
            println("Stage updateResultsFile")
            if (composeInfo != null){
                println("Fetching rp_launch_details")
                def rp_launch_details = [:]
                if (launch_id){
                    sh "sleep 60"
                    rp_launch_details = reportLib.fetchTestItemIdForLaunch(
                        launch_id,
                        rp_base_link,
                        rpPreprocDir,
                        credsRpProc,
                        metaData,
                        stageLevel,
                        run_type
                    )
                }
                reportLib.writeToResultsFile(
                    msgMap["artifact"]["version"],
                    run_type,
                    tierLevel,
                    stageLevel,
                    metaData['results'],
                    msgMap['run']['url'],
                    metaData['rp_link'],
                    rp_launch_details
                )
            }
        }

        stage('sendConsolidatedReport'){
            println("Stage sendConsolidatedReport")
            if (msgMap["pipeline"]["final_stage"] && tierLevel == "tier-2") {
                def rhcsVersion = sharedLib.getRHCSVersionFromArtifactsNvr()
                majorVersion = rhcsVersion["major_version"]
                minorVersion = rhcsVersion["minor_version"]
                minorVersion = "${minorVersion}"

                def testResults = sharedLib.readFromResultsFile(
                    msgMap["artifact"]["version"]
                )
                
                reportLib.updateConfluencePage(
                    sharedLib,
                    majorVersion,
                    minorVersion,
                    msgMap["artifact"]["version"],
                    run_type,
                    testResults
                )
                emailLib.sendConsolidatedEmail(
                    run_type,
                    metaData,
                    majorVersion,
                    minorVersion,
                    msgMap["artifact"]["version"]
                )
                sharedLib.sendGChatNotification(
                    run_type, metaData["results"], tierLevel, stageLevel, build_url
                )
            }
        }

        stage('sendUMBFortier0RC'){
            // This stage is to send UMB message for OSP interop team
            // everytime an RC build passes sanity tier-0
            println("Stage sendUMBFortier0RC")
            if (
                tags_list.contains("rc") && tierLevel == "tier-0"
                && msgMap["pipeline"]["final_stage"] && testStatus == "SUCCESS"
            ){
                def recipeMap = sharedLib.readFromReleaseFile(
                    majorVersion, minorVersion, lockFlag=false
                )
                umbLib = load("${env.WORKSPACE}/pipeline/vars/umb.groovy")
                umbLib.postUMBTestQueue(nvr, recipeMap, "false")
            }
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