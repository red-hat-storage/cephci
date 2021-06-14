/*
    Pipeline script for executing Tier 1 jobs for RH Ceph 5.0.
*/
// Global variables section

def nodeName = "centos-7"
def cephVersion = "pacific"
def sharedLib
def test_results = [:]
def composeInfo = ""
def tier1Jobs = ["rhceph-5-tier-1-deploy", "rhceph-5-tier-1-object", "rhceph-5-tier-1-rbd"]

// Pipeline script entry point
node(nodeName) {

	timeout(unit: "MINUTES", time: 30) {
		stage('Install prereq') {
			checkout([
                                $class: 'GitSCM',
                                branches: [[name: '*/master']],
                                doGenerateSubmoduleConfigurations: false,
                                extensions: [[
                                       $class: 'SubmoduleOption',
                                       disableSubmodules: false,
                                       parentCredentials: false,
                                       recursiveSubmodules: true,
                                       reference: '',
                                       trackingSubmodules: false
                                ]],
                                submoduleCfg: [],
                                userRemoteConfigs: [[url: 'https://github.com/red-hat-storage/cephci.git']]
                        ])
			script {
				sharedLib = load("${env.WORKSPACE}/pipeline/vars/common.groovy")
				sharedLib.prepareNode()
			}
		}
	}
	stage('Validate parameters'){
		def ciMessage = "${params.CI_MESSAGE}" ?: ""
		if (ciMessage?.trim()) {
			composeInfo = "${params.CI_MESSAGE}"
		}
		else{
			withEnv([
				"rhcephVersion=5.0-rhel-8"
			]){
				composeInfo = sharedLib.fetchTier1Compose()
			}
		}
		if(!composeInfo){
			currentBuild.result = 'ABORTED'
			error('Aborting the pipeline since the compose for tier-1 job execution does not exist')
		}
		println "Triggering tier-1 jobs for compose ${composeInfo}"
	}

	timeout(unit: "HOURS", time: 24) {
		script {
			for(jobName in tier1Jobs){
				stage("${jobName}"){
					def jobResult = build propagate: false, \
							job: jobName, \
							parameters: [[
								$class: 'StringParameterValue',
								name: 'CI_MESSAGE',
								value: composeInfo
							]]
					println "The job ${jobName} completed with status ${jobResult.result}"
					test_results[jobName] = jobResult.result
					catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
						if(jobResult.result != 'SUCCESS'){
							sh "exit 1"
						}
					}
				}
			}
		}
	}
	stage('Publish Results') {
		script {
			def ciValues = sharedLib.fetchComposeInfo(composeInfo)
			withEnv([
				"rhcephVersion=5.0-rhel-8",
				"composeId=${ciValues["composeId"]}",
				"composeUrl=${ciValues["composeUrl"]}",
				"repository=${ciValues["repository"]}"
			]){
				sharedLib.sendEMail("Tier-1", test_results, false)
				def result_set = test_results.values().toSet()
				if ( result_set.size() == 1 && ("SUCCESS" in test_results.values()) ){
					sharedLib.sendUMBMessage("Tier1TestingDone")
				}
				sharedLib.postTier1Compose(test_results, composeInfo)
			}
		}
	}
}

