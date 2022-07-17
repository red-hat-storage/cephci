/*
    Script that adds the unsigned RPM build url into the qe build information file.
*/
// Global variables section
def sharedLib
def cephVersion
def composeUrl
def platform

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
            sharedLib = load("${env.WORKSPACE}/pipeline/vars/v3.groovy")
            sharedLib.prepareNode()
        }

        stage('updateRecipeFile') {
            echo "${params.CI_MESSAGE}"

            ciMsg = sharedLib.getCIMessageMap()
            composeUrl = ciMsg["compose_url"]

            cephVersion = sharedLib.fetchCephVersion(composeUrl)
            versionInfo = sharedLib.fetchMajorMinorOSVersion("unsigned-compose")

            majorVer = versionInfo['major_version']
            minorVer = versionInfo['minor_version']
            platform = versionInfo['platform']

            releaseContent = sharedLib.readFromReleaseFile(majorVer, minorVer)
            if ( releaseContent?.latest?."ceph-version") {
                currentCephVersion = releaseContent["latest"]["ceph-version"]
                def compare = sharedLib.compareCephVersion(currentCephVersion, cephVersion)

                if (compare == -1) {
                    sharedLib.unSetLock(majorVer, minorVer)
                    currentBuild.result = "ABORTED"
                    println "Build Ceph Version: ${cephVersion}"
                    println "Found Ceph Version: ${currentCephVersion}"
                    error("The latest ceph version is lower than existing one.")
                }
            }

            if ( !releaseContent.containsKey("latest") ) {
                releaseContent.latest = [:]
                releaseContent.latest.composes = [:]
            }

            releaseContent["latest"]["ceph-version"] = cephVersion
            releaseContent["latest"]["composes"]["${platform}"] = composeUrl
            sharedLib.writeToReleaseFile(majorVer, minorVer, releaseContent)

            def bucket = "ceph-${majorVer}.${minorVer}-${platform}"
            sharedLib.uploadCompose(bucket, cephVersion, composeUrl)

        }

        stage('postUMB') {

            def msgMap = [
                "artifact": [
                    "type" : "product-build",
                    "name": "Red Hat Ceph Storage",
                    "version": cephVersion,
                    "nvr": "RHCEPH-${majorVer}.${minorVer}",
                    "build_action": "latest",
                    "phase": "tier-0"
                ],
                "contact":[
                    "name": "Downstream Ceph QE",
                    "email": "cephci@redhat.com"
                ],
                "build": [
                    "platform": platform,
                    "compose-url": composeUrl
                ],
                "run": [
                    "url": env.BUILD_URL,
                    "log": "${env.BUILD_URL}console"
                ],
                "version": "1.0.0"
            ]
            def topic = 'VirtualTopic.qe.ci.rhcephqe.product-build.update.complete'

            //send UMB message notifying that a new build has arrived
            sharedLib.SendUMBMessage(msgMap, topic, 'ProductBuildDone')
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
