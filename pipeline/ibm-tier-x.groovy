/*
    Pipeline script for executing IBM Tier x test suites for RH Ceph Storage.
*/

def nodeName = "agent-01"
def testStages = [:]
def testResults = [:]
def rhcephVersion
def buildArtifacts
def buildType
def buildPhase
def sharedLib


node(nodeName) {

    timeout(unit: "MINUTES", time: 30) {
        stage('prepareJenkinsAgent') {
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

        if ((! rhcephVersion?.trim()) && (! buildType?.trim())) {
            error "Required Parameters are not provided.."
        }

        // Check ceph version to continue tier-x execution
        def recipeFileContent = sharedLib.readFromRecipeFile(rhcephVersion)
        def content = recipeFileContent['latest']
        println "recipeFile ceph-version : ${content['ceph-version']}"
        println "buildArtifacts ceph-version : ${buildArtifacts['ceph-version']}"
        if ( buildArtifacts['ceph-version'] != content['ceph-version']) {
            currentBuild.result = "ABORTED"
            error "Aborting the execution as new builds are available.."
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

        // Removing suites that are meant to be executed only in RH network.
        testStages = testStages.findAll { ! it.key.contains("psi-only") }

        if ( testStages.isEmpty() ) {
            currentBuild.result = "ABORTED"
            error "No test stages found.."
        }
        currentBuild.description = "${params.rhcephVersion} - ${buildPhase}"
    }

    (testStages.keySet() as List).collate(5).each {
        def stages = testStages.subMap(it)
        parallel stages
    }

    stage('publishTestResults') {
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

        // Using only Tier-0 as pipeline progresses even if intermediate stages fail.
        def content = recipeMap["tier-0"]
        content["product"] = "Red Hat Ceph Storage"
        content["version"] = rhcephVersion
        content["date"] = sh(returnStdout: true, script: "date")
        content["log"] = env.RUN_DISPLAY_URL
        content["stage"] = buildPhase
        content["results"] = testResults

        writeYaml file: "${env.WORKSPACE}/${dirName}/metadata.yaml", data: content
        sharedLib.uploadResults(dirName, "${env.WORKSPACE}/${dirName}")

        def msgMap = [
            "artifact": [
                "type": "product-build",
                "name": "Red Hat Ceph Storage",
                "version": content["ceph-version"],
                "nvr": rhcephVersion,
                "phase": "testing",
                "build": "tier-0",
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
                "name": buildPhase,
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
                "type": buildType,
                "category": "functional",
                "result": currentBuild.currentResult,
                "object-prefix": dirName,
            ],
            "recipe": buildArtifacts,
            "generated_at": env.BUILD_ID,
            "version": "1.1.0",
        ]

        def msgType = "Tier2ValidationTestingDone"
        if ( buildPhase == "tier-1" ) {
            msgType = "Tier1TestingDone"
        }

        sharedLib.SendUMBMessage(
            msgMap,
            "VirtualTopic.qe.ci.rhcephqe.product-build.test.complete",
            msgType,
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
        buildArtifacts = writeJSON returnText: true, json: buildArtifacts
        // Update result to recipe file and execute post tier based on run execution
        build ([
            wait: false,
            job: "tier-x",
            parameters: [string(name: 'rhcephVersion', value: rhcephVersion),
                        string(name: 'buildType', value: buildPhase),
                        string(name: 'buildArtifacts', value: buildArtifacts)]
        ])

        if ("FAIL" in sharedLib.fetchStageStatus(testResults)) {
            currentBuild.result = "FAILED"
            error "Failure occurred in current run.."
        }

        sharedLib.writeToRecipeFile(buildType, rhcephVersion, buildPhase)
        latestContent = sharedLib.readFromRecipeFile(rhcephVersion)
        println "latest content is: ${latestContent}"
    }

}
