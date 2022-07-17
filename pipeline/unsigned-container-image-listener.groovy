/*
    Script to update development RHCS container image information to QE build recipes.
*/
// Global variables section
def lib
def versions
def cephVersion
def compose
def releaseDetails = [:]

// Pipeline script entry point

node("rhel-8-medium || ceph-qe-ci") {

    try {
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

            // prepare the node
            lib = load("${env.WORKSPACE}/pipeline/vars/v3.groovy")
            lib.prepareNode(1)
        }

        stage('updateRecipeFile') {
            println "msg = ${params.CI_MESSAGE}"

            compose = lib.getCIMessageMap()
            versions = lib.fetchMajorMinorOSVersion('unsigned-container-image')
            cephVersion = lib.fetchCephVersion(compose.compose_url)

            releaseDetails = lib.readFromReleaseFile(
                versions.major_version, versions.minor_version,
            )
            if ( !releaseDetails?.latest?."ceph-version") {
                lib.unSetLock(versions.major_version, versions.minor_version)
                currentBuild.results = "ABORTED"
                error("Unable to retrieve release information")
            }

            def currentCephVersion = releaseDetails.latest."ceph-version"
            def compare = lib.compareCephVersion(currentCephVersion, cephVersion)

            if (compare != 0) {
                lib.unSetLock(versions.major_version, versions.minor_version)
                currentBuild.result = "ABORTED"
                println "Build Ceph Version: ${cephVersion}"
                println "Found Ceph Version: ${currentCephVersion}"
                error("The ceph versions do not match.")
            }
            releaseDetails.latest.repository = compose.repository
            lib.writeToReleaseFile(
                versions.major_version, versions.minor_version, releaseDetails
            )

        }

        stage('postUMB') {
            def artifactsMap = [
                "artifact": [
                    "type": "product-build",
                    "name": "Red Hat Ceph Storage",
                    "version": cephVersion,
                    "nvr": "RHCEPH-${versions.major_version}.${versions.minor_version}",
                    "phase": "tier-0",
                    "build_action": "latest"
                ],
                "contact": [
                    "name": "Downstream Ceph QE",
                    "email": "cephci@redhat.com"
                ],
                "build": [
                    "repository": compose.repository,
                    "composes": releaseDetails.latest.composes,
                    "ceph-version": cephVersion
                ],
                "test": [
                    "phase": "tier-0"
                ],
                "run": [
                    "url": env.BUILD_URL,
                    "log": "${env.BUILD_URL}console"
                ],
                "version": "1.0.0"
            ]

            def msgContent = writeJSON returnText: true, json: artifactsMap
            println "${msgContent}"

            def overrideTopic = "VirtualTopic.qe.ci.rhcephqe.product-build.promote.complete"
            def msgType = "ProductBuildDone"

            lib.SendUMBMessage(msgContent, overrideTopic, msgType)
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
