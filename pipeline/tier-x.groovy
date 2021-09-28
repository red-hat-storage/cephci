/*
    Pipeline script for executing Tier x test suites for RH Ceph Storage.
*/

def nodeName = "centos-7"
def testStages = [:]
def testResults = [:]
def releaseContent = [:]
def buildPhase
def buildType
def ciMap
def sharedLib
def majorVersion
def minorVersion
def postTierLevel
def msgType
def buildPhaseValue


node(nodeName) {

    timeout(unit: "MINUTES", time: 30) {
        stage('Install prereq') {
            if (env.WORKSPACE) {
                sh script: "sudo rm -rf *"
            }
            checkout([
                $class: 'GitSCM',
                branches: [[name: '*/master']],
                doGenerateSubmoduleConfigurations: false,
                extensions: [[
                    $class: 'CloneOption',
                    shallow: true,
                    noTags: false,
                    reference: '',
                    depth: 0
                ]],
                submoduleCfg: [],
                userRemoteConfigs: [[
                    url: 'https://github.com/red-hat-storage/cephci.git'
                ]]
            ])

            // prepare the node
            sharedLib = load("${env.WORKSPACE}/pipeline/vars/lib.groovy")
            sharedLib.prepareNode()
        }
    }

    stage('Prepare-Stages') {
        /* Prepare pipeline stages using RHCEPH version */
        ciMap = sharedLib.getCIMessageMap()
        buildPhase = ciMap["artifact"]["phase"]
        buildType = ciMap["test-run"]["type"]
        def rhcsVersion = sharedLib.getRHCSVersionFromArtifactsNvr()
        majorVersion = rhcsVersion["major_version"]
        minorVersion = rhcsVersion["minor_version"]

        /*
           Read the release yaml contents to get contents,
           before other listener/Executor Jobs updates it.
        */
        releaseContent = sharedLib.readFromReleaseFile(
            majorVersion, minorVersion, lockFlag=false
        )

        // Till the pipeline matures, using the build that has passed tier-0 suite.
        testStages = sharedLib.fetchStages("tier-0", buildPhase, testResults)
        if ( testStages.isEmpty() ) {
            currentBuild.result = "ABORTED"
            error "No test stages found.."
        }
    }

    parallel testStages

    stage('Publish Results') {
        /* Publish results through E-mail and Google Chat */
        buildPhaseValue = buildPhase.split("-")
        def postTierValue = buildPhaseValue[1].toInteger()+1
        postTierLevel = buildPhaseValue[0]+"-"+postTierValue

        if ( ! ("FAIL" in testResults.values()) ) {
            def latestContent = sharedLib.readFromReleaseFile(
                majorVersion, minorVersion
            )
            if (releaseContent.containsKey(buildType)){
                if (latestContent.containsKey(buildPhase)){
                    latestContent[buildPhase] = releaseContent[buildType]
                }
                else {
                    def updateContent = ["${buildPhase}": releaseContent[buildType]]
                    latestContent += updateContent
                }
            }
            else {
                sharedLib.unSetLock(majorVersion, minorVersion)
                error "No data found for pre tier level: ${buildType}"
            }

            sharedLib.writeToReleaseFile(majorVersion, minorVersion, latestContent)
            println "latest content is: ${latestContent}"
        }

        sharedLib.sendGChatNotification(testResults, buildPhase.capitalize())
        sharedLib.sendEmail(
            testResults,
            sharedLib.buildArtifactsDetails(releaseContent, ciMap, "tier-0"),
            buildPhase.capitalize()
        )
    }

    stage('Publish UMB') {
        /* send UMB message */

        def artifactsMap = [
            "artifact": [
                "type": "product-build-${buildPhase}",
                "name": "Red Hat Ceph Storage",
                "version": ciMap["artifact"]["version"],
                "nvr": ciMap["artifact"]["nvr"],
                "phase": postTierLevel,
            ],
            "contact": [
                "name": "Downstream Ceph QE",
                "email": "ceph-qe@redhat.com",
            ],
            "pipeline": [
                "name": "rhceph-tier-x",
            ],
            "test-run": [
                "type": buildPhase,
                "result": currentBuild.currentResult,
                "url": env.BUILD_URL,
                "log": "${env.BUILD_URL}console",
            ],
            "version": "1.0.0"
        ]
        if (buildPhase == "tier-2"){msgType = "Tier2ValidationTestingDone"}
        else {msgType = buildPhaseValue[0].capitalize()+buildPhaseValue[1]+"TestingDone"}
        def msgContent = writeJSON returnText: true, json: artifactsMap
        println "${msgContent}"

        sharedLib.SendUMBMessage(
            artifactsMap,
            "VirtualTopic.qe.ci.rhcephqe.product-build.test.complete",
            msgType,
        )
    }

}
