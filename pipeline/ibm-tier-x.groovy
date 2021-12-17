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
            "--build tier-0 --cloud ibmc --xunit-results",
             buildPhase,
             testResults,
             rhcephversion
        )

        if ( testStages.isEmpty() ) {
            currentBuild.result = "ABORTED"
            error "No test stages found.."
        }
        currentBuild.description = "${params.rhcephVersion} - ${buildPhase}"
    }

    // Running the test suites in batches of 5
    (testStages.keySet() as List).collate(5).each{
        def stages = testStages.subMap(it)
        parallel stages
    }

    stage('upload xUnit-xml to COS'){
        def dirName = "ibm_${currentBuild.projectName}_${currentBuild.number}"
        testResults.each{key,dict->
            sharedLib.uploadObject(key, dirName, dict["log-dir"])
        }
    }

    stage('Update Results and Execute Tier-X suite') {
        /* Update result to recipe file and execute post tier based on run execution */
        if ("FAIL" in sharedLib.fetchStageStatus(testResults)) {
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
