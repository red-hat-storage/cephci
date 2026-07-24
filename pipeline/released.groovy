// Script to trigger when a RH Ceph is released and execute live tests in metadata file of RH Ceph release
// available in the external repository.
// Global variables section
def sharedLib
def versions
def cephVersion
def composeUrl
def containerImage
def rhcephVersion
def run_type = "Live"
def tierLevel = "live"
def stageLevel = null
def testStages = [:]
def testResults = [:]
def majorVersion
def minorVersion
def rp_base_link = "https://reportportal-rhcephqe.apps.ocp-c1.prod.psi.redhat.com"
def launch_id = ""


// Pipeline script entry point
node("rhel-9-medium || ceph-qe-ci") {

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
        println("reportLib : ${reportLib}")
    }

    stage("fetchTestSuites") {
        versions = sharedLib.fetchMajorMinorOSVersion("released")
        majorVersion = versions.major_version
        minorVersion = versions.minor_version
        def cimsg = sharedLib.getCIMessageMap()
        def repoDetails = cimsg.build.extra.image
        def overrides = ["build" : "released"]
        def workspace = "${env.WORKSPACE}"
        def build_number = "${currentBuild.number}"
        overrides.put("workspace", workspace.toString())
        overrides.put("build_number", build_number.toInteger())

        containerImage = repoDetails.index.pull.find({ x -> !(x.contains("sha")) })

        def repoUrl = repoDetails.yum_repourls.find({ x -> x.contains("RHCEPH") })
        composeUrl = repoUrl.split("work").find({
            x -> x.contains("RHCEPH-${majorVersion}.${minorVersion}")
        })
        println "repo url : ${composeUrl}"
        rhcephVersion = "${majorVersion}.${minorVersion}"
        println(rhcephVersion)

        cephVersion = sharedLib.fetchCephVersion(composeUrl)
        fetchStages = sharedLib.fetchStages(tierLevel, overrides, testResults, rhcephVersion)
        print("tests fetched")
        testStages = fetchStages["testStages"]
        final_stage = fetchStages["final_stage"]
        currentBuild.description = "RHCEPH-${majorVersion}.${minorVersion}"
    }

    parallel testStages

    stage('uploadTestResultToReportPortal'){

        // Copy all the results into one folder before upload
        def dirName = "psi_${currentBuild.projectName}_${currentBuild.number}"
        def targetDir = "${env.WORKSPACE}/${dirName}/results"
        def attachDir = "${env.WORKSPACE}/${dirName}/attachments"

        sh (script: "mkdir -p ${targetDir} ${attachDir}")
        print(testResults)
        testResults.each { key, value ->
            def fileName = key.replaceAll(" ", "-")
            def logDir = value["logdir"]
            sh "find ${logDir} -maxdepth 1 -type f -name xunit.xml -exec cp '{}' ${targetDir}/${fileName}.xml \\;"
            sh "tar -zcvf ${logDir}/${fileName}.tar.gz ${logDir}/*.log"
            sh "mkdir -p ${attachDir}/${fileName}"
            sh "cp ${logDir}/${fileName}.tar.gz ${attachDir}/${fileName}/"
            sh "find ${logDir} -maxdepth 1 -type f -not -size 0 -name '*.err' -exec cp '{}' ${attachDir}/${fileName}/ \\;"
        }

        println("directories created..")
        // Adding metadata information

        println("tierLevel : ${tierLevel}")
        println("cephVersion : ${cephVersion}")
        def content = [:]
        content["product"] = "Red Hat Ceph Storage"
        content["version"] = "RHCEPH-${majorVersion}.${minorVersion}"
        content["ceph_version"] = cephVersion
        content["date"] = sh(returnStdout: true, script: "date")
        content["log"] = env.RUN_DISPLAY_URL
        content["stage"] = tierLevel
        content["results"] = testResults

        println("content : ${content}")
        writeYaml file: "${env.WORKSPACE}/${dirName}/metadata.yaml", data: content
        sh "sudo cp -r ${env.WORKSPACE}/${dirName} /ceph/cephci-jenkins"

        println("configureReportPortalWorkDir")
        (rpPreprocDir, credsRpProc) = reportLib.configureRpPreProc(sharedLib)

        def resultDir = dirName
        println("Test results are available at ${resultDir}")

        def tmpDir = sh(returnStdout: true, script: "mktemp -d").trim()
        sh "sudo cp -r /ceph/cephci-jenkins/${resultDir} ${tmpDir}"

        metaData = readYaml file: "${tmpDir}/${resultDir}/metadata.yaml"
        println("metadata: ${metaData}")
        def copyFiles = "sudo cp -a ${tmpDir}/${resultDir}/results ${rpPreprocDir}/payload/"
        def copyAttachments = "sudo cp -a ${tmpDir}/${resultDir}/attachments ${rpPreprocDir}/payload/"
        def rmTmpDir = "sudo rm -rf ${tmpDir}"

        sh script: "${copyFiles} && ${copyAttachments} && ${rmTmpDir}"

        if (metaData["results"]) {
                launch_id = reportLib.uploadTestResults(rpPreprocDir, credsRpProc, metaData, stageLevel, run_type)
                println("launch_id: ${launch_id}")
                if (launch_id) {
                    metaData["rp_link"] = "${rp_base_link}/ui/#cephci/launches/all/${launch_id}"
                }
        }
    }

    stage('postResults') {
        def status = 'PASSED'
        if ("FAIL" in sharedLib.fetchStageStatus(testResults)) {
           status = 'FAILED'
        }
        build_url = env.BUILD_URL

        def contentMap = [
            "artifact": [
                "name": "Red Hat Ceph Storage",
                "nvr": "RHCEPH-${versions.major_version}.${versions.minor_version}",
                "phase": "released",
                "type": "released-build",
                "version": cephVersion,
                "rhcephVersion": rhcephVersion
            ],
            "build": [
                "repository": "cdn.redhat.com"
            ],
            "contact": [
                "email": "cephci@redhat.com",
                "name": "Downstream Ceph QE"
            ],
            "run": [
                "log": "${env.BUILD_URL}console",
                "url": env.BUILD_URL
            ],
            "test": [
                "category": "release",
                "result": status
            ],
            "version": "3.1.0"
        ]

        def msgContent = writeJSON returnText: true, json: contentMap
        def overrideTopic = "VirtualTopic.qe.ci.rhceph.test.complete"

        sharedLib.SendUMBMessage(msgContent, overrideTopic, "TestingCompleted")
        println "Updated UMB Message Successfully"

        def msg = [
            "product": "Red Hat Ceph Storage",
            "version": contentMap["artifact"]["nvr"],
            "ceph_version": contentMap["artifact"]["version"],
            "container_image": contentMap["build"]["repository"],
            "rhcephVersion": contentMap["artifact"]["rhcephVersion"]
        ]
        sharedLib.sendGChatNotification(
            run_type, testResults, tierLevel, stageLevel, build_url, rhcephVersion
        )

        sharedLib.sendEmail(
                run_type,
                testResults,
                msg,
                tierLevel
        )

    }
}
