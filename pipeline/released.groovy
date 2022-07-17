// Script to trigger when a RH Ceph is released and execute live tests in metadata file of RH Ceph release
// available in the external repository.
// Global variables section
def sharedLib
def versions
def cephVersion
def composeUrl
def containerImage
def rhcephVersion
def run_type = "Live"
def tierLevel = "live"
def stageLevel = null
def testStages = [:]
def testResults = [:]


// Pipeline script entry point
node("rhel-8-medium || ceph-qe-ci") {

    stage('Preparing') {
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

    stage("fetchTestSuites") {
        versions = sharedLib.fetchMajorMinorOSVersion("released")
        def majorVersion = versions.major_version
        def minorVersion = versions.minor_version
        def cimsg = sharedLib.getCIMessageMap()
        def repoDetails = cimsg.build.extra.image
        def overrides = ["build" : "released"]

        containerImage = repoDetails.index.pull.find({ x -> !(x.contains("sha")) })

        def repoUrl = repoDetails.yum_repourls.find({ x -> x.contains("RHCEPH") })
        composeUrl = repoUrl.split("work").find({
            x -> x.contains("RHCEPH-${majorVersion}.${minorVersion}")
        })
        println "repo url : ${composeUrl}"
        rhcephVersion = "${majorVersion}.${minorVersion}"
        println(rhcephVersion)

        cephVersion = sharedLib.fetchCephVersion(composeUrl)
        fetchStages = sharedLib.fetchStages(tierLevel, overrides, testResults, rhcephVersion)
        print("tests fetched")
        testStages = fetchStages["testStages"]
        final_stage = fetchStages["final_stage"]
        currentBuild.description = "RHCEPH-${majorVersion}.${minorVersion}"
    }

    parallel testStages

    stage('postResults') {
        def status = 'PASSED'
        if ("FAIL" in sharedLib.fetchStageStatus(testResults)) {
           status = 'FAILED'
        }
        build_url = env.BUILD_URL

        def contentMap = [
            "artifact": [
                "name": "Red Hat Ceph Storage",
                "nvr": "RHCEPH-${versions.major_version}.${versions.minor_version}",
                "phase": "released",
                "type": "released-build",
                "version": cephVersion,
                "rhcephVersion": rhcephVersion
            ],
            "build": [
                "repository": "cdn.redhat.com"
            ],
            "contact": [
                "email": "cephci@redhat.com",
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
            "version": "3.1.0"
        ]

        def msgContent = writeJSON returnText: true, json: contentMap
        def overrideTopic = "VirtualTopic.qe.ci.rhceph.test.complete"

        sharedLib.SendUMBMessage(msgContent, overrideTopic, "TestingCompleted")
        println "Updated UMB Message Successfully"

        def msg = [
            "product": "Red Hat Ceph Storage",
            "version": contentMap["artifact"]["nvr"],
            "ceph_version": contentMap["artifact"]["version"],
            "container_image": contentMap["build"]["repository"],
            "rhcephVersion": contentMap["artifact"]["rhcephVersion"]
        ]
        sharedLib.sendGChatNotification(
            run_type, testResults, tierLevel, stageLevel, build_url, rhcephVersion
        )

        sharedLib.sendEmail(
                run_type,
                testResults,
                msg,
                tierLevel
        )

    }
}
