// Script to update the RC information to QE build recipes.
// Global variables section
def sharedLib
def versions
def cephVersion
def composeUrl
def platform
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

        stage("Updating") {
            versions = sharedLib.fetchMajorMinorOSVersion("signed-compose")
            def majorVersion = versions.major_version
            def minorVersion = versions.minor_version
            platform = versions.platform

            cimsg = sharedLib.getCIMessageMap()
            composeUrl = cimsg["compose-path"].replace(
                "/mnt/redhat/", "http://download-node-02.eng.bos.redhat.com/"
            )
            cephVersion = sharedLib.fetchCephVersion(composeUrl)

            def releaseMap = sharedLib.readFromReleaseFile(majorVersion, minorVersion)
            if ( releaseMap.isEmpty() ) {
                sharedLib.unSetLock(majorVersion, minorVersion)
                currentBuild.result = "ABORTED"
                error "No release information was found."
            }

            if ( releaseMap?.rc?."ceph-version" ) {
                def currentCephVersion = releaseMap.rc."ceph-version"
                def compare = sharedLib.compareCephVersion(currentCephVersion, cephVersion)
                if (compare == -1) {
                    sharedLib.unSetLock(majorVersion, minorVersion)
                    println "Existing Ceph Version: ${currentCephVersion}"
                    println "Found Ceph Version: ${cephVersion}"

                    currentBuild.result = "ABORTED"
                    error "Abort: New version is lower than existing ceph version."
                }
            }

            if ( !releaseMap.containsKey("rc") ) {
                releaseMap.rc = [:]
                releaseMap.rc.composes = [:]
            }

            releaseMap.rc."ceph-version" = cephVersion
            releaseMap.rc."compose-label" = cimsg["compose-label"]
            releaseMap.rc.composes["${platform}"] = composeUrl
            sharedLib.writeToReleaseFile(majorVersion, minorVersion, releaseMap)

            def bucket = "ceph-${majorVersion}.${minorVersion}-${platform}"
            sharedLib.uploadCompose(bucket, cephVersion, composeUrl)

        }

        stage('Messaging') {
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
                    "platform": platform,
                    "compose-url": composeUrl
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
            def overrideTopic = "VirtualTopic.qe.ci.rhcephqe.product-build.update.complete"

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
            "composeLabel": "${cimsg['compose-label']}",
            "composeUrl": composeUrl
        ]
        def subject = "${env.JOB_NAME} ${currentBuild.result} for ${rhcephVersion} - ${cephVersion} (RC) Release Candidate build"
        emailLib.sendEmailForListener(rhcephVersion, cephVersion, composeInfo, cimsg, failureReason, subject)
        subject += "\n Jenkins URL: ${env.BUILD_URL}"
        googlechatnotification(url: "id:rhcephCIGChatRoom", message: subject)
    }
}
