// Script to update the RC information to QE build recipes.
// Global variables section
def nodeName = "centos-7"
def sharedLib
def versions
def cephVersion
def composeUrl

// Pipeline script entry point
node(nodeName) {

    timeout(unit: "MINUTES", time: 30) {
        stage('Preparing') {
            if (env.WORKSPACE) {
                deleteDir()
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
                    url: 'https://github.com/red-hat-storage/cephci.git']]
            ])
            sharedLib = load("${env.WORKSPACE}/pipeline/vars/lib.groovy")
            sharedLib.prepareNode(1)
        }
    }

    stage("Updating") {
        versions = sharedLib.fetchMajorMinorOSVersion("signed-compose")
        def majorVersion = versions.major_version
        def minorVersion = versions.minor_version
        def platform = versions.platform

        def cimsg = sharedLib.getCIMessageMap()
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
                "compose-url": composeUrl
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
        def overrideTopic = "VirtualTopic.qe.ci.rhcephqe.product-build.update.complete"

        sharedLib.SendUMBMessage(msgContent, overrideTopic, "ProductBuildDone")
        println "Updated UMB Message Successfully"
    }
}
