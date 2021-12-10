/*
    Pipeline script for executing Tier x test suites for RH Ceph Storage.
*/

def nodeName = "centos-7"
def testStages = [:]
def testResults = [:]
def releaseContent = [:]
def ciMap
def sharedLib
def majorVersion
def minorVersion
def tierLevel
def previousTierLevel


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
        previousTierLevel = ciMap.test.type

        tierTypeStrings = previousTierLevel.split("-")
        def tierLevelInt = tierTypeStrings[1].toInteger() + 1
        tierLevel = tierTypeStrings[0] + "-" + tierLevelInt

        // Once the pipeline matures, uncomment the below line
        // cliArg = "--build ${tierLevel}"
        def cliArgs = "--build tier-0"
        if ( ciMap.artifact.build == "signed" ) {
            cliArgs = "--build rc"
        }

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
        testStages = sharedLib.fetchStages(cliArgs, tierLevel, testResults)
        if ( testStages.isEmpty() ) {
            currentBuild.result = "ABORTED"
            error "No test stages found.."
        }

        currentBuild.description = "${ciMap.artifact.nvr} - ${ciMap.artifact.version} - ${tierLevel}"
    }

    parallel testStages

    stage('Publish Results') {
        /* Publish results through E-mail and Google Chat */

        if ( ! ("FAIL" in sharedLib.fetchStageStatus(testResults)) ) {
            def latestContent = sharedLib.readFromReleaseFile(
                majorVersion, minorVersion
            )
            if ( ! releaseContent.containsKey(previousTierLevel) ) {
                sharedLib.unSetLock(majorVersion, minorVersion)
                error "No data found for pre tier level: ${previousTierLevel}"
            }

            if ( latestContent.containsKey(tierLevel) ) {
                latestContent[tierLevel] = releaseContent[previousTierLevel]
            } else {
                def updateContent = [
                    "${tierLevel}": releaseContent[previousTierLevel]
                ]
                latestContent += updateContent
            }

            sharedLib.writeToReleaseFile(majorVersion, minorVersion, latestContent)
            println "latest content is: ${latestContent}"
        }

        sharedLib.sendGChatNotification(testResults, tierLevel.capitalize())
        sharedLib.sendEmail(
            testResults,
            sharedLib.buildArtifactsDetails(releaseContent, ciMap, "tier-0"),
            tierLevel.capitalize()
        )
    }

    stage('Publish UMB') {
        /* send UMB message */

        def artifactsMap = [
            "artifact": [
                "type": "product-build",
                "name": "Red Hat Ceph Storage",
                "version": ciMap["artifact"]["version"],
                "nvr": ciMap["artifact"]["nvr"],
                "phase": "testing",
                "build": ciMap.artifact.build,
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
                "name": "rhceph-tier-x",
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
            "version": "1.0.0"
        ]

        def msgType = "Tier1TestingDone"
        if ( tierLevel == "tier-2" ) {
            msgType = "Tier2ValidationTestingDone"
        }

        def msgContent = writeJSON returnText: true, json: artifactsMap
        println "${msgContent}"

        sharedLib.SendUMBMessage(
            artifactsMap,
            "VirtualTopic.qe.ci.rhcephqe.product-build.test.complete",
            msgType,
        )
    }

}
