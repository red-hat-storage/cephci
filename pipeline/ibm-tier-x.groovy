/*
    Pipeline script for executing IBM Tier x test suites for RH Ceph Storage.
*/

def nodeName = "agent-01"
def testStages = [:]
def testResults = [:]
def rhcephVersion
def buildType
def buildPhase
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
            error "Required Parameters are not provided.."
        }
        def buildPhaseValue = buildType.split("-")
        buildPhase = buildPhaseValue[1].toInteger()+1
        buildPhase = buildPhaseValue[0]+"-"+buildPhase
        // Till the pipeline matures, using the build that has passed tier-0 suite.
        testStages = sharedLib.fetchStages(
            "--build tier-0 --cloud ibmc", buildPhase, testResults, rhcephversion
        )
        if ( testStages.isEmpty() ) {
            currentBuild.result = "ABORTED"
            error "No test stages found.."
        }
        currentBuild.description = "${params.rhcephVersion} - ${buildPhase}"
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
            wait: false,
            job: "tier-x",
            parameters: [string(name: 'rhcephVersion', value: rhcephVersion),
                        string(name: 'buildType', value: buildPhase)]
        ])
    }

}
