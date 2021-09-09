/*
    Script that adds the unsigned RPM build url into the qe build information file.
*/
// Global variables section
def nodeName = "centos-7"
def sharedLib
def cephVersion
def composeUrl

// Pipeline script entry point
node(nodeName) {

    timeout(unit: "MINUTES", time: 30) {
        stage('Preparing') {
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
            sharedLib = load("${env.WORKSPACE}/pipeline/vars/lib.groovy")
            sharedLib.prepareNode(1)
        }
    }

    stage('Updating') {
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

    }

    stage('Messaging') {

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
                "email": "ceph-qe@redhat.com"
            ],
            "build": [
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

}
