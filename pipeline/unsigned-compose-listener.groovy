/*
    Script that adds the unsigned RPM build url into the qe build information file.
*/
// Global variables section
def sharedLib
def cephVersion
def composeUrl
def composeId
def platform
def ciMsg
def rhcephVersion
def emailLib

// Pipeline script entry point
node("rhel-9-medium || ceph-qe-ci") {
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
            emailLib = load("${env.WORKSPACE}/pipeline/vars/email.groovy")
            sharedLib.prepareNode()
        }

        stage('updateRecipeFile') {
            echo "${params.CI_MESSAGE}"

            ciMsg = sharedLib.getCIMessageMap()
            composeUrl = ciMsg["compose_url"]
            composeId = ciMsg["compose_id"]

            cephVersion = sharedLib.fetchCephVersion(composeUrl)
            versionInfo = sharedLib.fetchMajorMinorOSVersion("unsigned-compose")

            majorVer = versionInfo['major_version']
            minorVer = versionInfo['minor_version']
            platform = versionInfo['platform']
            rhcephVersion = "RHCEPH-${majorVer}.${minorVer}"

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
                    "nvr": rhcephVersion,
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
            composeInfo = [
                "composeId": composeId,
                "composeUrl": composeUrl
            ]
            def subject = "${env.JOB_NAME} ${currentBuild.result} for ${rhcephVersion} - ${cephVersion} build"
            emailLib.sendEmailForListener(rhcephVersion, cephVersion, composeInfo, ciMsg, failureReason, subject)
            googlechatnotification(url: "id:rhcephCIGChatRoom", message: "Jenkins URL: ${env.BUILD_URL}")
        }
    }
}
