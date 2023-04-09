/*
    Pipeline script for executing test suites that are meant to be executed only in PSI
    cloud.
*/
def ciMap
def sharedLib


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

        // prepare the node
        sharedLib = load("${env.WORKSPACE}/pipeline/vars/v3.groovy")
        sharedLib.prepareNode()
    }

    stage("executeWorkflow"){
        ciMap = sharedLib.getCIMessageMap()
        def rhcephVersion = ciMap.artifact.nvr
        def buildType = "tier-0"
        def tags = "sanity_openstack_only,tier-1,stage-1"
        def overrides = ["build": "tier-0"]
        def overridesStr = writeJSON returnText: true, json: overrides
        def buildArtifacts = "${params.CI_MESSAGE}"

        // Trigger psi_only pipeline based on ciMessage content
        if (ciMap.pipeline.final_stage && ciMap.pipeline.tags.contains("tier-0") && ciMap.test.result == "SUCCESS") {
            def recipeFileContent = sharedLib.yamlToMap("${rhcephVersion}.yaml")
            def content = recipeFileContent['latest']
            println "recipeFile ceph-version : ${content['ceph-version']}"
            println "buildArtifacts ceph-version : ${ciMap.recipe.'ceph-version'}"
            if ( ciMap.recipe."ceph-version" != content['ceph-version']) {
                currentBuild.result = "ABORTED"
                error "Aborting the execution as new builds are available.."
            }
            println "Starting test execution with parameters:"
            println "\trhcephVersion: ${rhcephVersion}\n\tbuildType: ${buildType}\n\tbuildArtifacts: ${buildArtifacts}\n\toverrides: ${overrides}\n\ttags: ${tags}"

            build ([
                wait: false,
                job: "rhceph-test-execution-pipeline",
                parameters: [
                    string(name: 'rhcephVersion', value: rhcephVersion.toString()),
                    string(name: 'tags', value: tags),
                    string(name: 'buildType', value: buildType),
                    string(name: 'overrides', value: overridesStr),
                    string(name: 'buildArtifacts', value: buildArtifacts['recipe'].toString())]
            ])
        }
        else
        {
            println("Failed to start the psi_only pipeline. CIMessage did not satisfy psi_only requirement.")
            println("ciMap.pipeline.final_stage : ${ciMap.pipeline.final_stage} \n ciMap.pipeline.tags : ${ciMap.pipeline.tags}")
            println("ciMap.test.result : ${ciMap.test.result}")
        }
    }
}
