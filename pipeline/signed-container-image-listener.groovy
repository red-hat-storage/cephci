// Script to update the container build information into QE recipes.
// Global variables section
def sharedLib
def versions
def cephVersion
def composeUrl
def containerImage
def releaseMap = [:]

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
            sharedLib.prepareNode(1)
        }

        stage("updateRecipeFile") {
            versions = sharedLib.fetchMajorMinorOSVersion("signed-container-image")
            def majorVersion = versions.major_version
            def minorVersion = versions.minor_version

            def cimsg = sharedLib.getCIMessageMap()
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
