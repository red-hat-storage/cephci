/*
    Script that trigger testsuite for upstream build
*/
// Global variables section
def sharedLib
def testStages = [:]
def testResults = [:]
def upstreamVersion = "${params.Upstream_Version}"
def currentStage = "${params.Current_Stage}"
def buildType = "upstream"
def yamlData
def cephVersion
def tags = "${buildType},${currentStage}"
def overrides = [
    "build": buildType,
    "upstream-build": upstreamVersion,
    "post-results": ""
]
def status = "STABLE"
def upstreamArtifact =  [:]
def failureReason
def reportPotalLink
def executionResult

// Pipeline script entry point
node('ceph-qe-ci || rhel-9-medium') {
    try {
        stage('Preparing') {
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
            reportLib = load("${env.WORKSPACE}/pipeline/vars/upload_results.groovy")
        }

        stage('Fetch Test-suites for Execution') {
            overrides.put("workspace", "${env.WORKSPACE}")
            yamlData = readYaml file: "/ceph/cephci-jenkins/latest-rhceph-container-info/${buildType}.yaml"
            cephVersion = yamlData[upstreamVersion]["ceph-version"]
            currentBuild.description = "${upstreamVersion} - ${currentStage} - ${cephVersion}"
            fetchStages = sharedLib.fetchStages(tags, overrides, testResults, null, null, upstreamVersion)
            testStages = fetchStages["testStages"]
            if ( testStages.isEmpty() ) {
                currentBuild.result = "FAILURE"
                error "Test suites were not found for execution"
            }
            print("Stages fetched: ${fetchStages}")
        }

        parallel testStages

        stage('uploadTestResultToReportPortal'){

            // Copy all the results into one folder before upload
            def rp_base_link = "https://reportportal-rhcephqe.apps.ocp-c1.prod.psi.redhat.com"
            def launch_id = ""
            def dirName = "upstream_${currentBuild.projectName}_${currentBuild.number}"
            def targetDir = "${env.WORKSPACE}/${dirName}/results"
            def attachDir = "${env.WORKSPACE}/${dirName}/attachments"

            sh (script: "mkdir -p ${targetDir} ${attachDir}")
            println(testResults)
            testResults.each { key, value ->
                def fileName = key.replaceAll(" ", "-")
                def logDir = value["logdir"]
                sh "find ${logDir} -maxdepth 1 -type f -name xunit.xml -exec cp '{}' ${targetDir}/${fileName}.xml \\;"
                sh "tar -zcvf ${logDir}/${fileName}.tar.gz ${logDir}/*.log"
                sh "mkdir -p ${attachDir}/${fileName}"
                sh "cp ${logDir}/${fileName}.tar.gz ${attachDir}/${fileName}/"
                sh "find ${logDir} -maxdepth 1 -type f -not -size 0 -name '*.err' -exec cp '{}' ${attachDir}/${fileName}/ \\;"
            }

            // Adding metadata information
            def content = [:]
            content["product"] = "Red Hat Ceph Storage"
            content["version"] = upstreamVersion
            content["ceph_version"] = cephVersion
            content["date"] = sh(returnStdout: true, script: "date")
            content["log"] = env.RUN_DISPLAY_URL
            content["stage"] = currentStage
            content["results"] = testResults
            println("content : ${content}")

            writeYaml file: "${env.WORKSPACE}/${dirName}/metadata.yaml", data: content
            sh "sudo cp -r ${env.WORKSPACE}/${dirName} /ceph/cephci-jenkins"
            (rpPreprocDir, credsRpProc) = reportLib.configureRpPreProc(sharedLib)
            println("Test results are available at ${dirName}")

            def tmpDir = sh(returnStdout: true, script: "mktemp -d").trim()
            sh "sudo cp -r /ceph/cephci-jenkins/${dirName} ${tmpDir}"
            metaData = readYaml file: "${tmpDir}/${dirName}/metadata.yaml"
            println("metadata: ${metaData}")

            def copyFiles = "sudo cp -a ${tmpDir}/${dirName}/results ${rpPreprocDir}/payload/"
            def copyAttachments = "sudo cp -a ${tmpDir}/${dirName}/attachments ${rpPreprocDir}/payload/"
            def rmTmpDir = "sudo rm -rf ${tmpDir}"
            sh script: "${copyFiles} && ${copyAttachments} && ${rmTmpDir}"

            launch_id = reportLib.uploadTestResults(rpPreprocDir, credsRpProc, metaData, currentStage, buildType)
            metaData["rp_link"] = "${rp_base_link}/ui/#cephci/launches/all/${launch_id}"
            println("metadata: ${metaData}")
            reportPotalLink = metaData["rp_link"]
            executionResult = metaData["results"]
        }

        stage('publish result') {
            upstreamArtifact = ["composes": yamlData[upstreamVersion]["composes"],
                                    "product": "Ceph Storage",
                                    "ceph_version": cephVersion,
                                    "repository": yamlData[upstreamVersion]["image"],
                                    "upstreamVersion": upstreamVersion]
            if ( "FAIL" in sharedLib.fetchStageStatus(testResults) ) {
                currentBuild.result = "FAILURE"
                status = "UNSTABLE"
                failureReason = "Failure found in suite execution of ${currentStage}"
            }
        }

        stage('Post Build Action') {
            nextStage = "stage-" + ((currentStage.split('-')[-1] as int) + 1)
            tags = "${buildType},${nextStage}"
            fetchStages = sharedLib.fetchStages(tags, overrides, testResults, null, null, upstreamVersion)
            testStages = fetchStages["testStages"]
            if ( !testStages.isEmpty() ) {
                build ([
                    wait: false,
                    job: "rhceph-upstream-test-executor",
                    parameters: [string(name: 'Upstream_Version', value: upstreamVersion.toString()),
                                string(name: 'Current_Stage', value: nextStage.toString())]
                ])
            }
        }
    } catch(Exception err) {
        // notify about failure
        currentBuild.result = "FAILURE"
        if (err.getMessage())
            failureReason = err.getMessage()
        echo failureReason
    } finally {
        def body = readFile(file: "pipeline/vars/emailable-report.html")
        body += "<body>"
        body += "<h3><u>Test Summary</u></h3>"
        body += "<table>"
        body += "<tr><th>Test Suite</th><th>Result</th></tr>"
        executionResult.each{k,v->
            def test_name = k.replace("-", " ")
            body += "<tr><td>${test_name.capitalize()}</td><td>${v['status']}</td></tr>"
        }
        body += "</table><br />"
        body += "<h3><u>Test Artifacts</u></h3>"
        body += "<table>"
        body += "<tr><td>Ceph Version</td><td>${cephVersion}</td></tr>"
        body += "<tr><td>Compose URL</td><td>${yamlData[upstreamVersion]['composes']}</td></tr>"
        body += "<tr><td>Container Image</td><td>${yamlData[upstreamVersion]['image']}</td></tr>"
        body += "<tr><td>Report portal URL</td><td>${reportPotalLink}</td></tr>"
        if (currentBuild.result != "SUCCESS") {
            body += "<tr><td>${currentBuild.result} Reason</td><td>${failureReason}</td></tr>"
        }
        body += "<tr><td>Jenkins Build</td><td>${env.BUILD_URL}</td></tr></table><br /></body></html>"
        def subject = "Upstream test report status of ${currentStage} ceph version:${cephVersion}-${upstreamVersion} is ${status}"
        emailext (
            mimeType: 'text/html',
            subject: "${subject}",
            body: "${body}",
            from: "cephci@redhat.com",
            to: "cephci@redhat.com"
        )
        subject += "\nJenkins URL: ${env.BUILD_URL}"
        if (currentBuild.result != "SUCCESS") {
            subject += "\n${currentBuild.result} Reason: ${failureReason}"
        }
        googlechatnotification(url: "id:rhcephCIGChatRoom", message: subject)
    }
}
