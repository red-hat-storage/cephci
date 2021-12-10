// Script to trigger when a RH Ceph is released and execute Tier-0 using the bits
// available in the external repository.
// Global variables section
def nodeName = "centos-7"
def sharedLib
def versions
def cephVersion
def composeUrl
def containerImage
def cliArgs = "--build released"
def tierLevel = "tier-0"
def testStages = [:]
def testResults = [:]


// Pipeline script entry point
node(nodeName) {

    timeout(unit: "MINUTES", time: 30) {
        stage('Preparing') {
            if (env.WORKSPACE) {
                sh script: "sudo rm -rf *"
            }
            checkout([
                $class: 'GitSCM',
                branches: [[ name: '*/master' ]],
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
            sharedLib.prepareNode()
        }
    }

    stage("Get test suites") {
        versions = sharedLib.fetchMajorMinorOSVersion("released")
        def majorVersion = versions.major_version
        def minorVersion = versions.minor_version

        def cimsg = sharedLib.getCIMessageMap()
        def repoDetails = cimsg.build.extra.image

        containerImage = repoDetails.index.pull.find({ x -> !(x.contains("sha")) })

        def repoUrl = repoDetails.yum_repourls.find({ x -> x.contains("RHCEPH") })
        composeUrl = repoUrl.split("work").find({
            x -> x.contains("RHCEPH-${majorVersion}.${minorVersion}")
        })
        println "repo url : ${composeUrl}"

        cephVersion = sharedLib.fetchCephVersion(composeUrl)
        testStages = sharedLib.fetchStages(cliArgs, tierLevel, testResults)

        currentBuild.description = "RHCEPH-${majorVersion}.${minorVersion}"
    }

    parallel testStages

    stage('Publish Results') {
        def status = 'PASSED'
        if ("FAIL" in sharedLib.fetchStageStatus(testResults)) {
           status = 'FAILED'
        }

        def contentMap = [
            "artifact": [
                "name": "Red Hat Ceph Storage",
                "nvr": "RHCEPH-${versions.major_version}.${versions.minor_version}",
                "phase": "released",
                "type": "released-build",
                "version": cephVersion
            ],
            "build": [
                "repository": "cdn.redhat.com"
            ],
            "contact": [
                "email": "ceph-qe@redhat.com",
                "name": "Downstream Ceph QE"
            ],
            "run": [
                "log": "${env.BUILD_URL}console",
                "url": env.BUILD_URL
            ],
            "test": [
                "category": "release",
                "result": status
            ],
            "version": "1.1.0"
        ]

        def msgContent = writeJSON returnText: true, json: contentMap
        def overrideTopic = "VirtualTopic.qe.ci.rhceph.test.complete"

        sharedLib.SendUMBMessage(msgContent, overrideTopic, "TestingCompleted")
        println "Updated UMB Message Successfully"

        def msg = [
            "product": "Red Hat Ceph Storage",
            "version": contentMap["artifact"]["nvr"],
            "ceph_version": contentMap["artifact"]["version"],
            "container_image": contentMap["build"]["repository"]
        ]

        sharedLib.sendEmail(testResults, msg, tierLevel.capitalize())
    }
}
