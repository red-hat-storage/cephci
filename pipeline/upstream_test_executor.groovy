/*
    Script that trigger testsuite for upstream build
*/
// Global variables section
def sharedLib
def testStages = [:]
def testResults = [:]
def upstreamVersion = "${params.Upstream_Version}"
def buildType = "upstream"

// Pipeline script entry point
node('ceph-qe-ci || rhel-8-medium') {
    try {
        timeout(unit: "MINUTES", time: 30) {
            stage('Preparing') {
                if (env.WORKSPACE) { sh script: "sudo rm -rf * .venv" }
                checkout(
                    scm: [
                        $class: 'GitSCM',
                        branches: [[name: 'origin/master']],
                        extensions: [
                            [
                                $class: 'CleanBeforeCheckout',
                                deleteUntrackedNestedRepositories: true
                            ],
                            [
                                $class: 'WipeWorkspace'
                            ],
                            [
                                $class: 'CloneOption',
                                depth: 1,
                                noTags: true,
                                shallow: true,
                                timeout: 10,
                                reference: ''
                            ]
                        ],
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
        }
        stage('Fetch Test-suites for Execution') {
            def tags = "${buildType},stage-1"
            def overrides = [build:buildType,"upstream-build":upstreamVersion]
            fetchStages = sharedLib.fetchStages(tags, overrides, testResults, null, null, upstreamVersion)
            testStages = fetchStages["testStages"]
            if ( testStages.isEmpty() ) {
                currentBuild.result = "FAILURE"
                error "No test suites were found for execution."
            }
            print("Stages fetched: ${fetchStages}")
            currentBuild.description = "${buildType} - ${upstreamVersion}"
        }

        parallel testStages

        stage('publish result') {
            if ( ! ("FAIL" in sharedLib.fetchStageStatus(testResults)) ) {
                println "Publish result"
            }
        }
    } catch(Exception err) {
        // notify about failure
        currentBuild.result = "FAILURE"
        def failureReason = err.getMessage()
        echo failureReason
    }
}
