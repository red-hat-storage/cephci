/*
    Script to update development RHCS container image information to QE build recipes.
*/
// Global variables section
def nodeName = "centos-7"
def lib
def versions
def cephVersion
def compose
def releaseDetails = [:]

// Pipeline script entry point

node(nodeName) {

    timeout(unit: "MINUTES", time: 30) {
        stage('Preparing') {
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

            // prepare the node
            lib = load("${env.WORKSPACE}/pipeline/vars/lib.groovy")
            lib.prepareNode(1)
        }
    }

    stage('Updating') {
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

    stage('Messaging') {
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
                "email": "ceph-qe@redhat.com"
            ],
            "build": [
                "repository": compose.repository
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

    stage("Push Ceph Recipe") {
        def recipeMap = [
            "ceph-version": cephVersion,
            "repository": compose.repository.tokenize(":")[-1],
            "RHCephVersion": "RHCEPH-${versions.major_version}.${versions.minor_version}",
            "platforms": releaseDetails.latest.composes.keySet().collect()
        ]
        lib.uploadBuildRecipe(recipeMap)
    }

}
