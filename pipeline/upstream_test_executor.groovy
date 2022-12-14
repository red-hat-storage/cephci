/*
    Script that trigger testsuite for upstream build
*/
// Global variables section
def sharedLib
def testStages = [:]
def testResults = [:]
def upstreamVersion = "${params.Upstream_Version}"
def currentStage = "${params.Current_Stage}"
def buildType = "upstream"
def yamlData
def cephVersion
def tags = "${buildType},${currentStage}"
def overrides = [build:buildType, "upstream-build":upstreamVersion, "post-results": "", "report-portal": ""]

// Pipeline script entry point
node('ceph-qe-ci || rhel-8-medium') {
    try {
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

        stage('Fetch Test-suites for Execution') {
            fetchStages = sharedLib.fetchStages(tags, overrides, testResults, null, null, upstreamVersion)
            testStages = fetchStages["testStages"]
            if ( testStages.isEmpty() ) {
                currentBuild.result = "FAILURE"
                error "Test suites were not found for execution"
            }
            print("Stages fetched: ${fetchStages}")
            yamlData = readYaml file: "/ceph/cephci-jenkins/latest-rhceph-container-info/${buildType}.yaml"
            cephVersion = yamlData[upstreamVersion]["ceph-version"]
            currentBuild.description = "${upstreamVersion} - ${currentStage} - ${cephVersion}"
        }

        parallel testStages

        stage('publish result') {
            def upstreamArtifact = ["composes": yamlData[upstreamVersion]["composes"],
                                    "product": "Ceph Storage",
                                    "ceph_version": cephVersion,
                                    "repository": yamlData[upstreamVersion]["image"],
                                    "upstreamVersion": upstreamVersion]
            def status = "STABLE"
            if ( "FAIL" in sharedLib.fetchStageStatus(testResults) ) {
                currentBuild.result = "FAILURE"
                status = "UNSTABLE"
            }
            sharedLib.sendEmail(buildType, testResults, upstreamArtifact, null, currentStage)
            def msg = "Upstream test report status of ${currentStage} ceph version:${cephVersion}-${upstreamVersion} is ${status} .Log:${env.BUILD_URL}"
            googlechatnotification(url: "id:rhcephCIGChatRoom", message: msg)
        }

        stage('Post Build Action') {
            nextStage = "stage-" + ((currentStage.split('-')[-1] as int) + 1)
            tags = "${buildType},${nextStage}"
            fetchStages = sharedLib.fetchStages(tags, overrides, testResults, null, null, upstreamVersion)
            testStages = fetchStages["testStages"]
            if ( !testStages.isEmpty() ) {
                build ([
                    wait: false,
                    job: "rhceph-upstream-test-executor",
                    parameters: [string(name: 'Upstream_Version', value: upstreamVersion.toString()),
                                string(name: 'Current_Stage', value: nextStage.toString())]
                ])
            }
        }

    } catch(Exception err) {
        // notify about failure
        currentBuild.result = "FAILURE"
        def failureReason = err.getMessage()
        echo failureReason
    }
}
