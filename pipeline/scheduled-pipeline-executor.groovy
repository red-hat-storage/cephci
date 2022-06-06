/*
    Pipeline script for executing weekly scheduled test suites
*/
// Global variables section

def sshServer = "ssh 10.245.4.89"

def recipeFile = "RHCEPH-*.yaml"
def recipeFileDir = "/data/site/recipe"
def buildType = "${params.buildType}" ?: "tier-1"
def overrides = "${params.overrides}" ?: "{}"
def tags = "${params.tags}" ?: ""
def tagsList = tags.split(',') as List
def nodeName = "centos-7"
if ("ibmc" in tagsList) {
    nodeName = "agent-01"
}

// Pipeline script entry point
node(nodeName) {

    stage("Trigger Pipeline") {
        println "Begin synchronzing image artifacts."

        def validRecipeFiles = sh(
                returnStdout: true,
                script: "${sshServer} \"find ${recipeFileDir} -name ${recipeFile} -mtime -7 -ls | awk '{print \$11}'\"")

        if (validRecipeFiles) {
            echo "There no new RHCS build since last Friday"
            exit 1
        }

        for (validRecipeFile in ${validRecipeFiles}) {
            def recipeContent = sh(
                    returnStdout: true,
                    script: "${sshServer} \"yq e '.latest' ${validRecipeFile}\"")
            def rhcephVersion = sh(
                    returnStdout: true,
                    script: "${sshServer} \"yq e '.latest.ceph-version' ${validRecipeFile}\"")

            println "Starting test execution with parameters:"
            println "rhcephVersion: ${rhcephVersion}, buidType: ${phase}, buildArtifacts: ${recipeContent}, overrides: ${overrides}, tags: ${tags}"

            build ([
                wait: false,
                job: "rhceph-tier-executor",
                parameters: [
                    string(name: 'rhcephVersion', value: rhcephVersion),
                    string(name: 'tags', value: tags),
                    string(name: 'buildType', value: buildType.toString()),
                    string(name: 'overrides', value: overrides.toString()),
                    string(name: 'buildArtifacts', value: buildArtifacts.toString())]
            ])
        }
    }
}
