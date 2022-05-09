/*
    Pipeline script for executing v3 pipeline
*/

def nodeName = "agent-01"
def testStages = [:]
def testResults = [:]
def sharedLib
def rhcephVersion
def tags
def tags_list
def overrides
def buildArtifacts
def buildType
def buildPhase

node(nodeName) {
        timeout(unit: "MINUTES", time: 30) {
            stage('Install prereq') {
                if (env.WORKSPACE) { sh script: "sudo rm -rf * .venv" }
                checkout(
                    scm: [
                        $class: 'GitSCM',
                        branches: [[name: 'origin/pipeline_demo']],
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
                            url: 'https://github.com/manasagowri/cephci.git'
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
                    nodeName = "centos-7"
                    sharedLib.prepareNode()
                }
            }
        }

        stage("PrepareTestStages") {
            /* Prepare pipeline stages using RHCEPH version */
            rhcephVersion = "${params.rhcephVersion}" ?: ""
            buildType = "${params.buildType}" ?: ""  //what are we getting from buildtype and buildphase?
            buildArtifacts = "${params.buildArtifacts}" ?: [:]
            tags = "${params.tags}" ?: ""
            overrides = "${params.overrides}" ?: "{}"
            println("Fetching buildArtifacts")
            println(buildArtifacts)

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

            // Till the pipeline matures, using the build that has passed tier-0 suite.
            print("Fetching stages")
            testStages = sharedLib.fetchStages(tags, overrides, testResults, rhcephVersion)
            print("Stages fetched")
            print(testStages)

            tags_list = tags.split(',') as List
            if ( testStages.isEmpty() ) {
                //periodical will be taken care in post build action
                if (!('periodical' in tags_list)){
                    def index = tags_list.findIndexOf { it ==~ /tier-\w+/ }
                    def tier_level = tags_list.get(index)
                    if(tier_level == "tier-2"){
                        currentBuild.result = "ABORTED"
                        error "No test stages found.."
                    }
                    def tierValue = tier_level.split("-")
                    Increment_tier= tierValue[1].toInteger()+1
                    tier_level= tierValue[0]+"-"+Increment_tier
                    tags_list.putAt(index,tier_level)
                    def index_stage = tags_list.findIndexOf { it ==~ /stage-\w+/ }
                    tags_list.putAt(index_stage,"stage-1")
                    tags=tags_list.join(",")
                    testStages = sharedLib.fetchStages(tags, overrides, testResults, rhcephVersion)
                    if (testStages.isEmpty()){
                        currentBuild.result = "ABORTED"
                        error "No test stages found.."
                    }
                }
                else{
                    currentBuild.result = "ABORTED"
                    error "No test stages found.."
                }
                currentBuild.description = "${params.rhcephVersion} - ${buildPhase}"
                println "currentBuild.description : ${currentBuild.description}"
            }
        }

        parallel stages

        stage('postBuildAction') {
            buildArtifacts = writeJSON returnText: true, json: buildArtifacts
            tags_list = tags.split(',') as List
            def index = tags_list.findIndexOf { it ==~ /stage-\w+/ }
            def stage_level = tags_list.get(index)
            def stageValue = stage_level.split("-")
            Increment_stage= stageValue[1].toInteger()+1
            stage_level= stageValue[0]+"-"+Increment_stage
            tags_list.putAt(index,stage_level)
            tags=tags_list.join(",")
            overrides = writeJSON returnText: true, json: overrides

            if ("FAIL" in sharedLib.fetchStageStatus(testResults)) {
                currentBuild.result = "FAILED"
                error "Failure occurred in current run.."
            }

            build ([
                wait: false,
                job: "rhceph-tier-executor",
                parameters: [string(name: 'rhcephVersion', value: rhcephVersion.toString()),
                            string(name: 'tags', value: tags.toString()),
                            string(name: 'buildType', value: buildType.toString()),
                            string(name: 'overrides', value: overrides.toString()),
                            string(name: 'buildArtifacts', value: buildArtifacts.toString())]
            ])

        }

}
