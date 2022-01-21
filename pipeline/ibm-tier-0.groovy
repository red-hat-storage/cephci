/* Pipeline script for executing Tier 0 test suites for RH Ceph Storage in IBM cloud. */

def nodeName = "agent-01"
def testStages = [:]
def testResults = [:]
def rhcephVersion
def buildType
def buildArtifacts
def buildPhase = "tier-0"
def sharedLib


node(nodeName) {

    timeout(unit: "MINUTES", time: 30) {
        stage('prepareNode') {
            if (env.WORKSPACE) { sh script: "sudo rm -rf * .venv" }
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

    stage('prepareTestStages') {
        /* Prepare pipeline stages using RHCEPH version */
        rhcephVersion = "${params.rhcephVersion}" ?: ""
        buildType = "${params.buildType}" ?: ""
        buildArtifacts = "${params.buildArtifacts}" ?: [:]

        if ( buildArtifacts ){
            buildArtifacts = readJSON text: "${buildArtifacts}"
        }

        if ( (! rhcephVersion?.trim()) && (! buildType?.trim()) ) {
            error "Required Prameters are not provided.."
        }

        testStages = sharedLib.fetchStages(
            "--build ${buildType} --cloud ibmc --xunit-results",
            buildPhase,
            testResults,
            rhcephVersion
        )

        if ( testStages.isEmpty() ) {
            currentBuild.result = "ABORTED"
            error "No test stages found.."
        }
        currentBuild.description = "${params.rhcephVersion} - ${buildPhase}"
    }

    parallel testStages

    stage('publishTestResult') {
        // Copy all the results into one folder before upload
        def dirName = "ibm_${currentBuild.projectName}_${currentBuild.number}"
        def targetDir = "${env.WORKSPACE}/${dirName}/results"
        sh(script: "mkdir -p ${targetDir}")
        testResults.each { key, value ->
            sh(
                script: "cp ${value['log-dir']}/xunit.xml ${targetDir}/${key}.xml",
                returnStatus: true
            )
        }

        // Adding metadata information
        def recipeMap = sharedLib.readFromRecipeFile(rhcephVersion)
        def content = recipeMap[buildType]
        content["product"] = "Red Hat Ceph Storage"
        content["version"] = rhcephVersion
        content["date"] = sh(returnStdout: true, script: "date")
        content["log"] = env.RUN_DISPLAY_URL
        content["stage"] = buildPhase
        content["results"] = testResults

        writeYaml file: "${env.WORKSPACE}/${dirName}/metadata.yaml", data: content
        sharedLib.uploadResults(dirName, "${env.WORKSPACE}/${dirName}")

        // Send UMB message
        def rpmType = "unsigned"
        if ( buildType == "rc" ) { rpmType = "signed" }

        def msgMap = [
            "artifact": [
                "type": "product-build",
                "name": "Red Hat Ceph Storage",
                "version": content["ceph-version"],
                "nvr": rhcephVersion,
                "phase": "testing",
                "build": rpmType,
            ],
            "contact": [
                "name": "Downstream Ceph QE",
                "email": "cephci@redhat.com",
            ],
            "system": [
                "os": "centos-7",
                "label": "agent-01",
                "provider": "IBM-Cloud",
            ],
            "pipeline": [
                "name": "tier-0",
                "id": currentBuild.number,
            ],
            "run": [
                "url": env.RUN_DISPLAY_URL,
                "additional_urls": [
                    "doc": "https://docs.engineering.redhat.com/display/rhcsqe/RHCS+QE+Pipeline",
                    "repo": "https://github.com/red-hat-storage/cephci",
                    "report": "https://reportportal-rhcephqe.apps.ocp4.prod.psi.redhat.com/",
                    "tcms": "https://polarion.engineering.redhat.com/polarion/",
                ],
            ],
            "test": [
                "type": buildPhase,
                "category": "functional",
                "result": currentBuild.currentResult,
                "object-prefix": dirName,
            ],
            "recipe": buildArtifacts,
            "generated_at": env.BUILD_ID,
            "version": "1.1.0",
        ]

        sharedLib.SendUMBMessage(
            msgMap,
            "VirtualTopic.qe.ci.rhcephqe.product-build.test.complete",
            "Tier0TestingDone",
        )

    }

    stage('postBuildAction') {
        // Archive the logs
        archiveArtifacts artifacts: "**/*.log"
        junit(
            testResults: "**/xunit.xml",
            skipPublishingChecks: true ,
            allowEmptyResults: true
        )

        // Update result to recipe file and execute post tier based on run execution
        if ("FAIL" in sharedLib.fetchStageStatus(testResults)) {
            currentBuild.result = "FAILED"
            error "Failure occurred in current run.."
        }
        sharedLib.writeToRecipeFile(buildType, rhcephVersion, buildPhase)
        latestContent = sharedLib.readFromRecipeFile(rhcephVersion)
        println "Recipe file content is: ${latestContent}"
        buildArtifacts = writeJSON returnText: true json: buildArtifacts
        if (buildType == 'latest') {
            build ([
                wait: false,
                job: "tier-x",
                parameters: [string(name: 'rhcephVersion', value: rhcephVersion),
                            string(name: 'buildType', value: buildPhase),
                            string(name: 'buildArtifacts', value: buildArtifacts)]
            ])
        }
    }
}
