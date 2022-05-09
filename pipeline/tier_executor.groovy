/*
    Pipeline script for executing v3 pipeline
*/

def testStages = [:]
def testResults = [:]
def sharedLib
def rhcephVersion
def tags_list
def tierLevel
def stageLevel
def overrides
def buildArtifacts
def buildType
def final_stage = false
def run_type
def nodeName = "centos-7"
def tags = "${params.tags}" ?: ""
tags_list = tags.split(',') as List
if ("ibmc" in tags_list){
    nodeName = "agent-01"
}

node(nodeName) {
        timeout(unit: "MINUTES", time: 30) {
            stage('Install prereq') {
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

                tags = "${params.tags}" ?: ""
                tags_list = tags.split(',') as List
                sharedLib = load("${env.WORKSPACE}/pipeline/vars/v3.groovy")
                if("ibmc" in tags_list){
                    sharedLib.prepareIbmNode()
                }
                else{
                    sharedLib.prepareNode()
                }
            }
        }

        stage("PrepareTestStages") {
            /* Prepare pipeline stages using RHCEPH version */
            rhcephVersion = "${params.rhcephVersion}" ?: ""
            println(rhcephVersion)
            buildType = "${params.buildType}" ?: ""
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

            overrides.put("build", "tier-0")
            if(tierLevel == "tier-0"){
                overrides.put("build", "latest")
            }

            if("ibmc" in tags_list){
                def workspace = "${env.WORKSPACE}"
                def build_number = "${currentBuild.number}"
                overrides.put("workspace", workspace.toString())
                overrides.put("build_number", build_number.toInteger())
            }
            print(overrides)

            def rhcsVersion = sharedLib.getRHCSVersionFromArtifactsNvr()
            majorVersion = rhcsVersion["major_version"]
            minorVersion = rhcsVersion["minor_version"]

            /*
               Read the release yaml contents to get contents,
               before other listener/Executor Jobs updates it.
            */
            releaseContent = sharedLib.readFromReleaseFile(
                majorVersion, minorVersion, lockFlag=false
            )
            println(releaseContent)
            // Till the pipeline matures, using the build that has passed tier-0 suite.
            print("Fetching stages")
            fetchStages = sharedLib.fetchStages(tags, overrides, testResults, rhcephVersion)
            print("Stages fetched")
            print(fetchStages)
            testStages = fetchStages["testStages"]
            final_stage = fetchStages["final_stage"]
            println("Final Stage: ${final_stage}")
            currentBuild.description = "${params.rhcephVersion} - ${tierLevel} - ${stageLevel}"
            println "currentBuild.description : ${currentBuild.description}"

            if ( testStages.isEmpty() ) {
                //periodical will be taken care in post build action
                if (!('periodical' in tags_list)){
                    if(tierLevel == "tier-2"){
                        currentBuild.result = "ABORTED"
                        error "No test stages found.."
                    }
                    def tierValue = tierLevel.split("-")
                    Increment_tier= tierValue[1].toInteger()+1
                    tierLevel= tierValue[0]+"-"+Increment_tier
                    tags_list.putAt(index,tierLevel)
                    def index_stage = tags_list.findIndexOf { it ==~ /stage-\w+/ }
                    tags_list.putAt(index_stage,"stage-1")
                    tags=tags_list.join(",")
                    overrides.put("build", "tier-0")
                    if(tierLevel == "tier-0"){
                        overrides.put("build", "latest")
                    }
                    testStages = sharedLib.fetchStages(tags, overrides, testResults, final_stage, rhcephVersion)
                    println("Final Stage: ${final_stage}")
                    if (testStages.isEmpty()){
                        currentBuild.result = "ABORTED"
                        error "No test stages found.."
                    }
                }
                else{
                    currentBuild.result = "ABORTED"
                    error "No test stages found.."
                }
                currentBuild.description = "${params.rhcephVersion} - ${tierLevel} - ${stageLevel}"
                println "currentBuild.description : ${currentBuild.description}"
            }
        }

        parallel testStages

        if ("openstack" in tags_list){
            stage('Publish Results') {
            /* Publish results through E-mail and Google Chat */
                ciMap = sharedLib.getCIMessageMap()
                previousTierLevel = ciMap.test.type
                def index = tags_list.findIndexOf { it ==~ /stage-\w+/ }
                stageLevel = tags_list.get(index)

                if ( ! ("FAIL" in sharedLib.fetchStageStatus(testResults)) ) {
                    def latestContent = sharedLib.readFromReleaseFile(
                        majorVersion, minorVersion
                    )
                    if ( ! releaseContent.containsKey(previousTierLevel) ) {
                        sharedLib.unSetLock(majorVersion, minorVersion)
                        error "No data found for pre tier level: ${previousTierLevel}"
                    }

                    if ( latestContent.containsKey(tierLevel) ) {
                        latestContent[tierLevel] = releaseContent[previousTierLevel]
                    } else {
                        def updateContent = [
                            "${tierLevel}": releaseContent[previousTierLevel]
                        ]
                        latestContent += updateContent
                    }

                    sharedLib.writeToReleaseFile(majorVersion, minorVersion, latestContent)
                    println "latest content is: ${latestContent}"
                }

                run_type = "Schedule Run"
                if ("sanity" in tags_list){
                    run_type = "Sanity Run"
                }
                sharedLib.sendGChatNotification(run_type, testResults, tierLevel.capitalize(), stageLevel.capitalize())
                sharedLib.sendEmail(
                    run_type,
                    testResults,
                    sharedLib.buildArtifactsDetails(releaseContent, ciMap, overrides.get("build")),
                    tierLevel.capitalize(),
                    stageLevel.capitalize(),
                )
            }

            stage('Publish UMB') {
                /* send UMB message */

                def artifactsMap = [
                    "artifact": [
                        "type": "product-build",
                        "name": "Red Hat Ceph Storage",
                        "version": ciMap["artifact"]["version"],
                        "nvr": ciMap["artifact"]["nvr"],
                        "phase": "testing",
                        "build": ciMap.artifact.build,
                    ],
                    "contact": [
                        "name": "Downstream Ceph QE",
                        "email": "cephci@redhat.com",
                    ],
                    "system": [
                        "os": "centos-7",
                        "label": "centos-7",
                        "provider": "openstack",
                    ],
                    "pipeline": [
                        "name": "rhceph-tier-x",
                        "id": currentBuild.number,
                        "tags": tags,
                        "overrides": overrides,
                    ],
                    "run": [
                        "url": env.BUILD_URL,
                        "log": "${env.BUILD_URL}console",
                        "additional_urls": [
                            "doc": "https://docs.engineering.redhat.com/display/rhcsqe/RHCS+QE+Pipeline",
                            "repo": "https://github.com/red-hat-storage/cephci",
                            "report": "https://reportportal-rhcephqe.apps.ocp4.prod.psi.redhat.com/",
                            "tcms": "https://polarion.engineering.redhat.com/polarion/",
                        ],
                    ],
                    "test": [
                        "type": tierLevel,
                        "category": "functional",
                        "result": currentBuild.currentResult,
                    ],
                    "generated_at": env.BUILD_ID,
                    "version": "3.0.0"
                ]

                def msgType = "Tier1TestingDone"
                def msgContent = writeJSON returnText: true, json: artifactsMap
                println "${msgContent}"
                println(msgType)

                sharedLib.SendUMBMessage(
                    artifactsMap,
                    "VirtualTopic.qe.ci.rhcephqe.product-build.test.complete",
                    msgType,
                )
            }
            stage('postBuildAction') {
                buildArtifacts = writeJSON returnText: true, json: buildArtifacts
                tags_list = tags.split(',') as List
                def index = tags_list.findIndexOf { it ==~ /stage-\w+/ }
                stageLevel = tags_list.get(index)
                def stageValue = stageLevel.split("-")
                Increment_stage= stageValue[1].toInteger()+1
                stageLevel= stageValue[0]+"-"+Increment_stage
                tags_list.putAt(index,stageLevel)
                tags=tags_list.join(",")
                overrides = writeJSON returnText: true, json: overrides

                if ("FAIL" in sharedLib.fetchStageStatus(testResults)) {
                    currentBuild.result = "FAILED"
                    error "Failure in current build"
                }

                build ([
                    wait: false,
                    job: "rhceph-tier-executor",
                    parameters: [string(name: 'rhcephVersion', value: rhcephVersion.toString()),
                                string(name: 'tags', value: tags),
                                string(name: 'buildType', value: buildType.toString()),
                                string(name: 'overrides', value: overrides.toString()),
                                string(name: 'buildArtifacts', value: buildArtifacts.toString())]
                ])
            }
        }

        if("ibmc" in tags_list){
            stage('publishTestResults') {
                // Copy all the results into one folder before upload
                def dirName = "ibm_${currentBuild.projectName}_${currentBuild.number}"
                def targetDir = "${env.WORKSPACE}/${dirName}/results"
                def attachDir = "${env.WORKSPACE}/${dirName}/attachments"

                sh (script: "mkdir -p ${targetDir} ${attachDir}")
                print(testResults)
                testResults.each { key, value ->
                    def logDir = value["logdir"]
                    sh "find ${logDir} -maxdepth 1 -type f -name xunit.xml -exec cp '{}' ${targetDir}/${key}.xml \\;"
                    sh "tar -zcvf ${logDir}/${key}.tar.gz ${logDir}/*.log"
                    sh "mkdir -p ${attachDir}/${key}"
                    sh "cp ${logDir}/${key}.tar.gz ${attachDir}/${key}/"
                    sh "find ${logDir} -maxdepth 1 -type f -not -size 0 -name '*.err' -exec cp '{}' ${attachDir}/${key}/ \\;"
                }

                // Adding metadata information
                def recipeMap = sharedLib.readFromRecipeFile(rhcephVersion)

                // Using only Tier-0 as pipeline progresses even if intermediate stages fail.
                def content = recipeMap[tierLevel]
                content["product"] = "Red Hat Ceph Storage"
                content["version"] = rhcephVersion
                content["date"] = sh(returnStdout: true, script: "date")
                content["log"] = env.RUN_DISPLAY_URL
                content["stage"] = tierLevel
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
                        "result": currentBuild.currentResult,
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
                buildArtifacts = writeJSON returnText: true, json: buildArtifacts
                if (final_stage){
                    def tierValue = tierLevel.split("-")
                    Increment_tier= tierValue[1].toInteger()+1
                    tierLevel= tierValue[0]+"-"+Increment_tier
                    def index = tags_list.findIndexOf { it ==~ /tier-\w+/ }
                    tags_list.putAt(index,tierLevel)
                    def index_stage = tags_list.findIndexOf { it ==~ /stage-\w+/ }
                    tags_list.putAt(index_stage,"stage-1")
                    tags=tags_list.join(",")
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

                if ("FAIL" in sharedLib.fetchStageStatus(testResults)) {
                    currentBuild.result = "FAILED"
                }
                // Update result to recipe file and execute post tier based on run execution
                if(!final_stage || (final_stage && tierLevel != "tier-3")){
                    build ([
                        wait: false,
                        job: "rhceph-tier-executor",
                        parameters: [string(name: 'rhcephVersion', value: rhcephVersion.toString()),
                                    string(name: 'tags', value: tags),
                                    string(name: 'buildType', value: buildType.toString()),
                                    string(name: 'overrides', value: overrides.toString()),
                                    string(name: 'buildArtifacts', value: buildArtifacts.toString())]
                    ])
                }

                if(final_stage){
                    sharedLib.writeToRecipeFile(buildType, rhcephVersion, tierLevel)
                    latestContent = sharedLib.readFromRecipeFile(rhcephVersion)
                    println "latest content is: ${latestContent}"
                }                
            }


        }
}

