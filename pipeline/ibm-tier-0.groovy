/*
    Pipeline script for executing Tier x test suites for RH Ceph Storage.
*/

def nodeName = "agent-01"
def testStages = [:]
def testResults = [:]
def rhcephVersion
def buildType
def buildPhase = "tier-0"
def sharedLib


node(nodeName) {

    timeout(unit: "MINUTES", time: 30) {
        stage('Install prereq') {
            if (env.WORKSPACE) {
                sh script: "sudo rm -rf *"
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
                    url: 'https://github.com/red-hat-storage/cephci.git'
                ]]
            ])

            // prepare the node
            sharedLib = load("${env.WORKSPACE}/pipeline/vars/lib.groovy")
            sharedLib.prepareIbmNode()
        }
    }

    stage('Prepare-Stages') {
        /* Prepare pipeline stages using RHCEPH version */
        rhcephVersion = "${params.rhcephVersion}" ?: ""
        buildType = "${params.buildType}" ?: ""
        if ((! rhcephVersion?.trim()) && (! buildType?.trim())) {
            error "Required Prameters are not provided.."
        }
        testStages = sharedLib.fetchStages("--build latest --cloud ibmc --skip-subscription", buildPhase, testResults, rhcephversion)
        if ( testStages.isEmpty() ) {
            currentBuild.result = "ABORTED"
            error "No test stages found.."
        }
    }

    parallel testStages

    stage('Update Results and Execute Tier-X suite') {
        /* Update result to recipe file and execute post tier based on run execution */
        if ("FAIL" in testResults.values()) {
            currentBuild.result = "FAILED"
            error "Failure occurred in current run.."
        }
        sharedLib.writeToRecipeFile(buildType, rhcephVersion, buildPhase)
        latestContent = sharedLib.readFromRecipeFile(rhcephVersion)
        println "latest content is: ${latestContent}"
        build ([
            wait: true,
            job: "tier-x",
            parameters: [string(name: 'rhcephVersion', value: rhcephVersion),
                        string(name: 'buildType', value: buildPhase)]
        ])
    }
}
