// Pipeline script for executing Tier-3 test suites

// Recipe file path details
def recipeFile = "RHCEPH-*.yaml"
def recipeFileDir = "/ceph/cephci-jenkins/latest-rhceph-container-info"

// Default job parameters
def buildType = (params.buildType)?.trim() ?: "tier-1"
def overrides = (params.overrides)?.trim() ?: "{}"
def tags = (params.tags)?.trim() ?: "schedule,openstack-only,tier-3,stage-1"

// Pipeline script entry point
node("rhel-9-medium || ceph-qe-ci") {
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
        load("${env.WORKSPACE}/pipeline/vars/v3.groovy").prepareNode(1)
    }

    stage("executeWorkflow") {
        println("Begin synchronizing image artifacts.")

        def validRecipeFiles = sh(
            returnStdout: true,
            script: "find ${recipeFileDir} -name ${recipeFile} -mtime -7 -print"
        ).trim().split("\n") as List

        if (! validRecipeFiles ) {
            currentBuild.result = 'ABORTED'
            error("There no new RHCS build since last Friday")
        }

        for (validRecipeFile in validRecipeFiles) {
            def rhcephVersion = validRecipeFile.split("/").last().replace(".yaml", "")
            def recipeContent = readYaml file: "${validRecipeFile}"

            if ( ! recipeContent.containsKey('tier-1') ) {
                println('No stable build available for ${rhcephVersion}')
                continue
            }

            def recipeString = writeJSON returnText: true, json: recipeContent['tier-1']

            println("Triggering test execution:")
            println("\tRHCEPH Version: ${rhcephVersion}")
            println("\tBuild: ${buildType}")
            println("\tRecipe: ${recipeContent['tier-1']}")
            println("\tOverrides: ${overrides}")
            println("\tMetadata tags: ${tags}")

            build ([
                wait: false,
                job: "rhceph-test-execution-pipeline",
                parameters: [
                    string(name: 'rhcephVersion', value: rhcephVersion),
                    string(name: 'tags', value: tags),
                    string(name: 'buildType', value: buildType),
                    string(name: 'overrides', value: overrides),
                    string(name: 'buildArtifacts', value: recipeString)
                ]
            ])
        }
    }
}
