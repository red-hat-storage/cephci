// Script to update the container build information into QE recipes.
// Global variables section
def sharedLib
def versions
def cephVersion
def composeUrl
def containerImage
def releaseMap = [:]
def failureReason = ""
def cimsg = ""
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

        stage("updateRecipeFile") {
            versions = sharedLib.fetchMajorMinorOSVersion("signed-container-image")
            def majorVersion = versions.major_version
            def minorVersion = versions.minor_version

            cimsg = sharedLib.getCIMessageMap()
            def repoDetails = cimsg.build.extra.image

            containerImage = repoDetails.index.pull.find({ x -> !(x.contains("sha")) })

            if(! repoDetails.containsKey("yum_repourls")){
                sharedLib.unSetLock(majorVersion, minorVersion)
                currentBuild.result = "ABORTED"
                error "Unable to retrieve compose url."
            }
            def repoUrl = repoDetails.yum_repourls.find({ x -> x.contains("Tools") })
            composeUrl = repoUrl.split("work").find({
                x -> x.contains("RHCEPH-${majorVersion}.${minorVersion}")
            })
            println "repo url : ${composeUrl}"

            cephVersion = sharedLib.fetchCephVersion(composeUrl)
            releaseMap = sharedLib.readFromReleaseFile(majorVersion, minorVersion)

            if ( releaseMap.isEmpty() || !releaseMap?.rc?."ceph-version" ) {
                sharedLib.unSetLock(majorVersion, minorVersion)
                currentBuild.result = "ABORTED"
                error "Unable to retrieve release build recipes."
            }

            def currentCephVersion = releaseMap.rc."ceph-version"
            def compare = sharedLib.compareCephVersion(currentCephVersion, cephVersion)
            if ( compare != 0 ) {
                sharedLib.unSetLock(majorVersion, minorVersion)
                println "Current Ceph Version : ${currentCephVersion}"
                println "New Ceph Version : ${cephVersion}"
                currentBuild.result = "ABORTED"
                error "Abort: Compose information is not updated yet for the current ceph version."
            }

            releaseMap.rc.repository = containerImage
            sharedLib.writeToReleaseFile(majorVersion, minorVersion, releaseMap)

            println "Success: Updated below information \n ${releaseMap}"
        }

        stage('postUMB') {
            def contentMap = [
                "artifact": [
                    "build_action": "rc",
                    "name": "Red Hat Ceph Storage",
                    "nvr": "RHCEPH-${versions.major_version}.${versions.minor_version}",
                    "phase": "rc",
                    "type": "product-build",
                    "version": cephVersion
                ],
                "build": [
                    "repository": containerImage,
                    "ceph-version": cephVersion,
                    "composes": releaseMap.rc.composes
                ],
                "contact": [
                    "email": "cephci@redhat.com",
                    "name": "Downstream Ceph QE"
                ],
                "run": [
                    "log": "${env.BUILD_URL}console",
                    "url": env.BUILD_URL
                ],
                "version": "1.0.0"
            ]
            def msgContent = writeJSON returnText: true, json: contentMap
            def overrideTopic = "VirtualTopic.qe.ci.rhcephqe.product-build.promote.complete"

            sharedLib.SendUMBMessage(msgContent, overrideTopic, "ProductBuildDone")
            println "Updated UMB Message Successfully"
            currentBuild.result = "SUCCESS"
        }
    } catch(Exception err) {
        failureReason = err.getMessage()
        if (currentBuild.result != "ABORTED") {
            currentBuild.result = "FAILURE"
            println "Failure Reason: ${failureReason}"
        }
    } finally {
        rhcephVersion = "RHCEPH-${versions.major_version}.${versions.minor_version}"
        composeInfo = [
            "composeLabel": "${releaseMap.rc.'compose-label'}",
            "containerImage": containerImage
        ]
        def subject = "${env.JOB_NAME} ${currentBuild.result} for ${rhcephVersion} - ${cephVersion} (RC) Release Candidate build"
        emailLib.sendEmailForListener(rhcephVersion, cephVersion, composeInfo, cimsg, failureReason, subject)
        subject += "\n Jenkins URL: ${env.BUILD_URL}"
        googlechatnotification(url: "id:rhcephCIGChatRoom", message: subject)
    }
}
