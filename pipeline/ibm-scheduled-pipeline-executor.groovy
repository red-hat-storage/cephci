/*
    Pipeline script for executing weekly scheduled test suites
*/

// Server to get recipe file contents
def SSHServer = "ssh 10.245.4.89"

// Set default values
def nodeName = "agent-01"

// Recipe file path details
def recipeFile = "RHCEPH-*.yaml"
def recipeFileDir = "/data/site/recipe"

// Default job parameters
def buildType = "${params.buildType}" ? "tier-1" : "${params.buildType}"
def overrides = "${params.overrides}" ? "{}" : "${params.overrides}"
def tags = "${params.tags}" ? "schedule,ibmc,tier-1,stage-1" : "${params.tags}"


// Pipeline script entry point
node(nodeName) {
    stage("Trigger Pipeline") {
        println "Begin synchronzing image artifacts."

        def validRecipeFiles = sh(
                returnStdout: true, script: "${SSHServer} \"find ${recipeFileDir} -name ${recipeFile} -mtime -7 -print\"").trim().split("\n") as List
        if (!validRecipeFiles) {
            currentBuild.result = 'ABORTED'
            error("There no new RHCS build since last Friday")
        }

        for (validRecipeFile in validRecipeFiles) {
            def rhcephVersion = validRecipeFile.split("/").last().replace(".yaml", "")
            def recipeContent = sh(
                    returnStdout: true, script: "${SSHServer} \"yq e '.tier-0' ${validRecipeFile}\"").trim()
            recipeContent = readYaml text: recipeContent
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
                    string(name: 'buildArtifacts', value: recipeContent.toString())]
            ])
        }
    }
}
