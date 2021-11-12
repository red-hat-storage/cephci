/*
    Pipeline script for executing Tier 0 test suites for RH Ceph Storage.
*/
// Global variables section

def nodeName = "centos-7"
def tierLevel = "tier-0"
def testStages = [:]
def testResults = [:]
def releaseContent = [:]
def buildType
def ciMap
def sharedLib
def majorVersion
def minorVersion


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
        buildType = ciMap["artifact"]["build_action"]
        cliArg = "--build ${buildType}"
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
        testStages = sharedLib.fetchStages(cliArg, tierLevel, testResults)
        if ( testStages.isEmpty() ) {
            currentBuild.result = "ABORTED"
            error "No test scripts were found for execution."
        }

        currentBuild.description = "${ciMap.artifact.nvr} - ${ciMap.artifact.version}"
    }

    parallel testStages

    stage('Publish Results') {
        /* Publish results through E-mail and Google Chat */

        def sourceKey = "latest"
        def updateKey = "tier-0"

        if ( ! ("FAIL" in testResults.values()) ) {
            def latestContent = sharedLib.readFromReleaseFile(
                majorVersion, minorVersion
            )

            if ( buildType == "rc" ) {
                sourceKey = "rc"
                updateKey = "rc"
            }

            if ( latestContent.containsKey(tierLevel) ) {
                latestContent[tierLevel] = releaseContent[sourceKey]
            }
            else {
                def updateContent = ["${tierLevel}": releaseContent[sourceKey]]
                latestContent += updateContent
            }
            sharedLib.writeToReleaseFile(majorVersion, minorVersion, latestContent)
        }

        sharedLib.sendGChatNotification(testResults, updateKey.capitalize())
        sharedLib.sendEmail(testResults, sharedLib.buildArtifactsDetails(
            releaseContent, ciMap, sourceKey), tierLevel.capitalize()
        )
    }

    stage('Publish UMB') {
        /* send UMB message */
        if ( "FAIL" in testResults.values() ) {
            // As part of executing all tiers, we are not publishing this message when
            // there is a failure. This way we prevent execution of Tier-1 and
            // subsequently the other tiers in the pipeline.
            println "Not posting the UMB message..."
            return
        }

        def artifactBuild
        if (ciMap.artifact.build_action == "rc") {
            artifactBuild = "signed"
        } else {
            artifactBuild = "unsigned"
        }

        def artifactsMap = [
            "artifact": [
                "type": "product-build",
                "name": "Red Hat Ceph Storage",
                "version": ciMap["artifact"]["version"],
                "nvr": ciMap["artifact"]["nvr"],
                "phase": "testing",
                "build": artifactBuild,
            ],
            "contact": [
                "name": "Downstream Ceph QE",
                "email": "cephci@redhat.com",
            ],
            "system": [
                "os": "centos-7",
                "label": "centos-7",
                "provider": "openstack",
            ],
            "pipeline": [
                "name": "rhceph-tier-0",
                "id": currentBuild.number,
            ],
            "run": [
                "url": env.BUILD_URL,
                "log": "${env.BUILD_URL}console",
                "additional_urls": [
                    "doc": "https://docs.engineering.redhat.com/display/rhcsqe/RHCS+QE+Pipeline",
                    "repo": "https://github.com/red-hat-storage/cephci",
                    "report": "https://reportportal-rhcephqe.apps.ocp4.prod.psi.redhat.com/",
                    "tcms": "https://polarion.engineering.redhat.com/polarion/",
                ],
            ],
            "test": [
                "type": tierLevel,
                "category": "functional",
                "result": currentBuild.currentResult,
            ],
            "generated_at": env.BUILD_ID,
            "version": "1.1.0",
        ]

        def msgContent = writeJSON returnText: true, json: artifactsMap
        println "${msgContent}"

        sharedLib.SendUMBMessage(
            artifactsMap,
            "VirtualTopic.qe.ci.rhcephqe.product-build.test.complete",
            "Tier0TestingDone",
        )
    }

}
