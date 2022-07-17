/*
    Pipeline script for executing weekly scheduled test suites
*/
// Recipe file path details
def recipeFile = "RHCEPH-*.yaml"
def recipeFileDir = "/ceph/cephci-jenkins/latest-rhceph-container-info"

// Default job parameters
def buildType = "${params.buildType}" ? "tier-1" : "${params.buildType}"
def overrides = "${params.overrides}" ? "{}" : "${params.overrides}"
def tags = "${params.tags}" ? "schedule,psi,tier-1,stage-1" : "${params.tags}"


// Pipeline script entry point
node("rhel-8-medium || ceph-qe-ci") {
    stage('prepareNode') {
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

        load("${env.WORKSPACE}/pipeline/vars/v3.groovy").prepareNode()
    }

    stage("executeWorkflow") {
        println "Begin synchronizing image artifacts."

        def validRecipeFiles = sh(
                returnStdout: true, script: "find ${recipeFileDir} -name ${recipeFile} -mtime -7 -print").trim().split("\n") as List
        if (!validRecipeFiles) {
            currentBuild.result = 'ABORTED'
            error("There no new RHCS build since last Friday")
        }

        for (validRecipeFile in validRecipeFiles) {
            def rhcephVersion = validRecipeFile.split("/").last().replace(".yaml", "")
            def recipeContent = readYaml file: "${validRecipeFile}"
            recipeContent = writeJSON returnText: true, json:  recipeContent

            println "Starting test execution with parameters:"
            println "\trhcephVersion: ${rhcephVersion}\n\tbuildType: ${buildType}\n\tbuildArtifacts: ${recipeContent}\n\toverrides: ${overrides}\n\ttags: ${tags}"

            build ([
                wait: false,
                job: "rhceph-test-execution-pipeline",
                parameters: [
                    string(name: 'rhcephVersion', value: rhcephVersion),
                    string(name: 'tags', value: tags),
                    string(name: 'buildType', value: buildType.toString()),
                    string(name: 'overrides', value: overrides.toString()),
                    string(name: 'buildArtifacts', value: recipeContent.get("tier-0").toString())]
            ])
        }
    }
}
