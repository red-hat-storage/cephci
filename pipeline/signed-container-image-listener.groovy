// Script to update the container build information into QE recipes.
// Global variables section
def nodeName = "centos-7"
def sharedLib
def versions
def cephVersion
def composeUrl
def containerImage

// Pipeline script entry point
node(nodeName) {

    timeout(unit: "MINUTES", time: 30) {
        stage('Preparing') {
            if (env.WORKSPACE) {
                sh script: "sudo rm -rf *"
            }
            checkout([
                $class: 'GitSCM',
                branches: [[name: 'origin/master']],
                doGenerateSubmoduleConfigurations: false,
                extensions: [
                    [
                        $class: 'CloneOption',
                        shallow: true,
                        noTags: true,
                        reference: '',
                        depth: 1
                    ],
                    [$class: 'CleanBeforeCheckout'],
                ],
                submoduleCfg: [],
                userRemoteConfigs: [[
                    url: 'https://github.com/red-hat-storage/cephci.git'
                ]]
            ])
            sharedLib = load("${env.WORKSPACE}/pipeline/vars/lib.groovy")
            sharedLib.prepareNode(1)
        }
    }

    stage("Updating") {
        versions = sharedLib.fetchMajorMinorOSVersion("signed-container-image")
        def majorVersion = versions.major_version
        def minorVersion = versions.minor_version

        def cimsg = sharedLib.getCIMessageMap()
        def repoDetails = cimsg.build.extra.image

        containerImage = repoDetails.index.pull.find({ x -> !(x.contains("sha")) })

        def repoUrl = repoDetails.yum_repourls.find({ x -> x.contains("Tools") })
        composeUrl = repoUrl.split("work").find({
            x -> x.contains("RHCEPH-${majorVersion}.${minorVersion}")
        })
        println "repo url : ${composeUrl}"

        cephVersion = sharedLib.fetchCephVersion(composeUrl)
        def releaseMap = sharedLib.readFromReleaseFile(majorVersion, minorVersion)

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
        }

        releaseMap.rc.repository = containerImage
        sharedLib.writeToReleaseFile(majorVersion, minorVersion, releaseMap)

        println "Success: Updated below information \n ${releaseMap}"
    }

    stage('Publishing') {
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
                "repository": containerImage
            ],
            "contact": [
                "email": "ceph-qe@redhat.com",
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
}
