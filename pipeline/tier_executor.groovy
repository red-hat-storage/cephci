/*
    Pipeline script for executing v3 pipeline
*/
def testStages = [:]
def testResults = [:]
def sharedLib
def rhcephVersion
def tags_list
def tierLevel
def tierNumber
def stageLevel
def currentStageLevel = ""
def overrides
def buildArtifacts
def buildType
def final_stage = false
def run_type
def nodeName = "rhel-8-medium || ceph-qe-ci"
def tags = ""
def majorVersion
def minorVersion
def buildStatus = "pass"

def branch='origin/master'
def repo='https://github.com/red-hat-storage/cephci.git'

if (params.containsKey('gitbranch')){
    branch=params.gitbranch
}

if (params.containsKey('gitrepo')){
    repo=params.gitrepo
}

if (params.containsKey('tags')){
    tags=params.tags
    tags_list = tags.split(',') as List
    if (tags_list.contains('ibmc')){
        nodeName = "agent-01 || ceph-qe-ci"
    }
}

node(nodeName) {
    stage('PrepareAgent') {
        if (env.WORKSPACE) { sh script: "sudo rm -rf * .venv" }
        checkout(
            scm: [
                $class: 'GitSCM',
                branches: [[name: branch]],
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
                    url: repo
                ]]
            ],
            changelog: false,
            poll: false
        )

        sharedLib = load("${env.WORKSPACE}/pipeline/vars/v3.groovy")
        if(tags_list.contains('ibmc')) {
            sharedLib.prepareIbmNode()
        }
        else {
            sharedLib.prepareNode()
        }
    }

    stage("PrepareTestStages") {
        /* Prepare pipeline stages using RHCEPH version */
        rhcephVersion = "${params.rhcephVersion}" ?: ""
        println(rhcephVersion)
        buildType = "${params.buildType}" ?: "latest"
        buildArtifacts = "${params.buildArtifacts}" ?: [:]
        tags = "${params.tags}" ?: ""
        overrides = "${params.overrides}" ?: "{}"
        println("Fetching buildArtifacts")
        if ( buildArtifacts ){
            buildArtifacts = readJSON text: "${buildArtifacts}"
        }
        println "buildArtifacts : ${buildArtifacts}"
        if ( (! rhcephVersion?.trim()) && (! tags?.trim()) ) {
            error "Required Parameters are not provided.."
        }
        if (overrides){
            overrides = readJSON text: "${overrides}"
        }
        tags_list = tags.split(',') as List
        def index = tags_list.findIndexOf { it ==~ /tier-\w+/ }
        tierLevel = tags_list.get(index)

        tierNumber = tierLevel.split("-") as List
        tierNumber = tierNumber[1] as Integer
        if (tierNumber > 2){
            error "Executions for tiers above tier 2 is not supported in this pipeline"
        }
        def stageIndex = tags_list.findIndexOf { it ==~ /stage-\w+/ }
        currentStageLevel = tags_list.get(stageIndex)

        run_type = "Schedule Run"
        if ("sanity" in tags_list){
            run_type = "Sanity Run"
        }
        if ("rc" in tags_list){
            run_type = "RC build Sanity Run"
        }

        overrides.put("build", "tier-0")
        if(tierLevel == "tier-0"){
            overrides.put("build", "latest")
        }
        if ("rc" in tags_list){
            overrides.put("build", "rc")
        }

        if ("openstack" in tags_list || "openstack-only" in tags_list){
            (majorVersion, minorVersion) = rhcephVersion.substring(7,).tokenize(".")
            /*
               Read the release yaml contents to get contents,
               before other listener/Executor Jobs updates it.
            */
            releaseContent = sharedLib.readFromReleaseFile(
                majorVersion, minorVersion, lockFlag=false
            )
            println("releaseContent")
            println(releaseContent)
            def workspace = "${env.WORKSPACE}"
            def build_number = "${currentBuild.number}"
            overrides.put("workspace", workspace.toString())
            overrides.put("build_number", build_number.toInteger())
        }

        if("ibmc" in tags_list) {
            def workspace = "${env.WORKSPACE}"
            def build_number = "${currentBuild.number}"
            overrides.put("workspace", workspace.toString())
            overrides.put("build_number", build_number.toInteger())
        }
        print(overrides)
        // Till the pipeline matures, using the build that has passed tier-0 suite.

        if (tags_list.containsAll(["ibmc","sanity"]) && (tierLevel != "tier-0")){
            def recipeFileContent = sharedLib.readFromRecipeFile(rhcephVersion)
            def content = recipeFileContent['latest']
            println "recipeFile ceph-version : ${content['ceph-version']}"
            println "buildArtifacts ceph-version : ${buildArtifacts['ceph-version']}"
            if ( buildArtifacts['ceph-version'] != content['ceph-version']) {
                currentBuild.result = "ABORTED"
                error "Aborting the execution as new builds are available.."
            }
        }

        print("Fetching stages")
        fetchStages = sharedLib.fetchStages(tags, overrides, testResults, rhcephVersion)
        print("Stages fetched")
        print(fetchStages)
        testStages = fetchStages["testStages"]
        final_stage = fetchStages["final_stage"]
        println("final_stage : ${final_stage}")
        ceph_version = buildArtifacts['ceph-version'] ?: buildArtifacts['recipe']['ceph-version']
        currentBuild.description = "${params.rhcephVersion} - ${tierLevel} - ${currentStageLevel} \n ceph-version : ${ceph_version}"
    }

    parallel testStages

    if ("openstack" in tags_list || "openstack-only" in tags_list){
        stage('Publish Results') {

            // Copy all the results into one folder before upload
            def dirName = "psi_${currentBuild.projectName}_${currentBuild.number}"
            def targetDir = "${env.WORKSPACE}/${dirName}/results"
            def attachDir = "${env.WORKSPACE}/${dirName}/attachments"

            sh (script: "mkdir -p ${targetDir} ${attachDir}")
            print(testResults)
            testResults.each { key, value ->
                def fileName = key.replaceAll(" ", "-")
                def logDir = value["logdir"]
                sh "find ${logDir} -maxdepth 1 -type f -name xunit.xml -exec cp '{}' ${targetDir}/${fileName}.xml \\;"
                sh "tar -zcvf ${logDir}/${fileName}.tar.gz ${logDir}/*.log"
                sh "mkdir -p ${attachDir}/${fileName}"
                sh "cp ${logDir}/${fileName}.tar.gz ${attachDir}/${fileName}/"
                sh "find ${logDir} -maxdepth 1 -type f -not -size 0 -name '*.err' -exec cp '{}' ${attachDir}/${fileName}/ \\;"
            }

            println("directories created..")

            // Adding metadata information
            def recipeMap = sharedLib.readFromReleaseFile(
                majorVersion, minorVersion, lockFlag=false
            )
            println("recipeMap : ${recipeMap}")
            println("tierLevel : ${tierLevel}")

            // Using only Tier-0 as pipeline progresses even if intermediate stages fail.
            def tier = "latest"
            if(tierLevel != "tier-0"){
                tier = "tier-0"
            }
            println("tier: ${tier}")
            def content = recipeMap[tier]
            content["product"] = "Red Hat Ceph Storage"
            content["version"] = rhcephVersion
            content["date"] = sh(returnStdout: true, script: "date")
            content["log"] = env.RUN_DISPLAY_URL
            content["stage"] = tierLevel
            content["results"] = testResults

            writeYaml file: "${env.WORKSPACE}/${dirName}/metadata.yaml", data: content
            sh "sudo cp -r ${env.WORKSPACE}/${dirName} /ceph/cephci-jenkins"

            run_type = "Manual Run"
            if ("openstack-only" in tags_list){
                if ("schedule" in tags_list){
                    run_type = "PSI-Only Schedule Run"
                } else {
                    run_type = "PSI-Only Sanity Run"
                }
            }

            testStatus = "SUCCESS"
            if ("FAIL" in sharedLib.fetchStageStatus(testResults)) {
                currentBuild.result = "FAILED"
                buildStatus = "fail"
                testStatus = "FAILURE"
            }

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
                    "name": tierLevel,
                    "id": currentBuild.number,
                    "tags": tags,
                    "overrides": overrides,
                    "run_type": run_type,
                    "final_stage": final_stage,
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
                    "result": testStatus,
                    "object-prefix": dirName,
                ],
                "recipe": buildArtifacts['recipe'],
                "generated_at": env.BUILD_ID,
                "version": "3.0.0",
            ]

            def msgType = "Custom"

            sharedLib.SendUMBMessage(
                msgMap,
                "VirtualTopic.qe.ci.rhcephqe.product-build.test.complete",
                msgType,
            )
        }

        stage('postBuildAction') {
            println("Inside post build action")
            buildArtifacts = writeJSON returnText: true, json: buildArtifacts
            def nextbuildType = buildType
            if (final_stage){
                def tierValue = tierLevel.split("-")
                Increment_tier= tierValue[1].toInteger()+1
                nextTierLevel= tierValue[0]+"-"+Increment_tier
                def index = tags_list.findIndexOf { it ==~ /tier-\w+/ }
                tags_list.putAt(index,nextTierLevel)
                def index_stage = tags_list.findIndexOf { it ==~ /stage-\w+/ }
                tags_list.putAt(index_stage,"stage-1")
                tags=tags_list.join(",")
                nextbuildType = tierLevel
            }
            else{
                tags_list = tags.split(',') as List
                def index = tags_list.findIndexOf { it ==~ /stage-\w+/ }
                stageLevel = tags_list.get(index)
                def stageValue = stageLevel.split("-")
                Increment_stage= stageValue[1].toInteger()+1
                stageLevel= stageValue[0]+"-"+Increment_stage
                tags_list.putAt(index,stageLevel)
                tags=tags_list.join(",")
            }
            overrides = writeJSON returnText: true, json: overrides

            // Execute post tier based on run execution
            // Do not trigger next stage of execution
            // if the current stage was final_stage of a particular tier and tier is greater than 2
            //    because the pipeline supports execution only till tier-2, tier-3 onwards will be part of system test
            if(!final_stage || (final_stage && tierNumber < 2)){
                build ([
                    wait: false,
                    job: "rhceph-test-execution-pipeline",
                    parameters: [string(name: 'rhcephVersion', value: rhcephVersion.toString()),
                                string(name: 'tags', value: tags),
                                string(name: 'buildType', value: nextbuildType.toString()),
                                string(name: 'overrides', value: overrides.toString()),
                                string(name: 'buildArtifacts', value: buildArtifacts.toString()),
                                string(name: 'gitrepo', value: repo.toString()),
                                string(name: 'gitbranch', value: branch.toString())]
                ])
            }
        }
    }

    if("ibmc" in tags_list) {
        println("Inside ibmc post results")
        stage('publishTestResults') {
            // Copy all the results into one folder before upload
            def dirName = "ibm_${currentBuild.projectName}_${currentBuild.number}"
            def targetDir = "${env.WORKSPACE}/${dirName}/results"
            def attachDir = "${env.WORKSPACE}/${dirName}/attachments"

            sh (script: "mkdir -p ${targetDir} ${attachDir}")
            print(testResults)
            testResults.each { key, value ->
                def fileName = key.replaceAll(" ", "-")
                def logDir = value["logdir"]
                sh "find ${logDir} -maxdepth 1 -type f -name xunit.xml -exec cp '{}' ${targetDir}/${fileName}.xml \\;"
                sh "tar -zcvf ${logDir}/${fileName}.tar.gz ${logDir}/*.log"
                sh "mkdir -p ${attachDir}/${fileName}"
                sh "cp ${logDir}/${fileName}.tar.gz ${attachDir}/${fileName}/"
                sh "find ${logDir} -maxdepth 1 -type f -not -size 0 -name '*.err' -exec cp '{}' ${attachDir}/${fileName}/ \\;"
            }

            println("directories created")
            // Adding metadata information
            def recipeMap = sharedLib.readFromRecipeFile(rhcephVersion)
            println("recipeMap : ${recipeMap}")
            println("tierLevel : ${tierLevel}")

            // Using only Tier-0 as pipeline progresses even if intermediate stages fail.
            def tier = "latest"
            if(tierLevel != "tier-0"){
                tier = "tier-0"
            }
            println("tier: ${tier}")
            def content = recipeMap[tier]
            content["product"] = "Red Hat Ceph Storage"
            content["version"] = rhcephVersion
            content["date"] = sh(returnStdout: true, script: "date")
            content["log"] = env.RUN_DISPLAY_URL
            content["stage"] = tierLevel
            content["results"] = testResults

            writeYaml file: "${env.WORKSPACE}/${dirName}/metadata.yaml", data: content
            sharedLib.uploadResults(dirName, "${env.WORKSPACE}/${dirName}")

            testStatus = "SUCCESS"
            if ("FAIL" in sharedLib.fetchStageStatus(testResults)) {
                currentBuild.result = "FAILED"
                buildStatus = "fail"
                testStatus = "FAILURE"
            }

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
                    "name": tierLevel,
                    "id": currentBuild.number,
                    "tags": tags,
                    "overrides": overrides,
                    "run_type": run_type,
                    "final_stage": final_stage,
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
                    "result": testStatus,
                    "object-prefix": dirName,
                ],
                "recipe": buildArtifacts,
                "generated_at": env.BUILD_ID,
                "version": "3.0.0",
            ]

            def msgType = "Custom"

            sharedLib.SendUMBMessage(
                msgMap,
                "VirtualTopic.qe.ci.rhcephqe.product-build.test.complete",
                msgType,
            )

        }
        stage('postBuildAction') {
            println("Inside post build action")
            buildArtifacts = writeJSON returnText: true, json: buildArtifacts
            def nextbuildType = buildType
            if (final_stage){
                def tierValue = tierLevel.split("-")
                Increment_tier= tierValue[1].toInteger()+1
                nextTierLevel= tierValue[0]+"-"+Increment_tier
                def index = tags_list.findIndexOf { it ==~ /tier-\w+/ }
                tags_list.putAt(index,nextTierLevel)
                def index_stage = tags_list.findIndexOf { it ==~ /stage-\w+/ }
                tags_list.putAt(index_stage,"stage-1")
                tags=tags_list.join(",")
                nextbuildType = tierLevel
            }
            else{
                tags_list = tags.split(',') as List
                def index = tags_list.findIndexOf { it ==~ /stage-\w+/ }
                stageLevel = tags_list.get(index)
                def stageValue = stageLevel.split("-")
                Increment_stage= stageValue[1].toInteger()+1
                stageLevel= stageValue[0]+"-"+Increment_stage
                tags_list.putAt(index,stageLevel)
                tags=tags_list.join(",")
            }
            overrides = writeJSON returnText: true, json: overrides

            // Do not trigger next stage of execution
            // 1) if the current tier executed is tier-0 and it failed
            // 2) if the current stage was final_stage of a particular tier and tier is greater than 2
            //    because the pipeline supports execution only till tier-2, tier-3 onwards will be part of system test
            if((!(tierLevel == "tier-0" && buildStatus == "fail")) &&
               (!final_stage || (final_stage && tierNumber < 2))){
                build ([
                    wait: false,
                    job: "rhceph-test-execution-pipeline",
                    parameters: [string(name: 'rhcephVersion', value: rhcephVersion.toString()),
                                string(name: 'tags', value: tags),
                                string(name: 'buildType', value: nextbuildType.toString()),
                                string(name: 'overrides', value: overrides.toString()),
                                string(name: 'buildArtifacts', value: buildArtifacts.toString())]
                ])
            }

            if(tierLevel == "tier-0" && final_stage && buildStatus == "pass"){
                sharedLib.writeToRecipeFile(rhcephVersion, tierLevel)
            }

            latestContent = sharedLib.readFromRecipeFile(rhcephVersion)
            println "latest content is: ${latestContent}"
        }
    }
}
