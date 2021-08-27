/*
    Pipeline script for executing Tier 2 object test suites for RH Ceph 5.0.
*/
// Global variables section

def nodeName = "centos-7"
def cephVersion = "pacific"
def sharedLib
def testResults = [:]
def defaultRHCSVersion = "5.0-rhel-8"
def jobs = [ "rhceph-5-tier-2-object-regression" ]
def ciMessage

// Pipeline script entry point

node(nodeName) {

    timeout(unit: "MINUTES", time: 30) {
        stage('Configure node') {
            checkout ([
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
                userRemoteConfigs: [[
                    url: 'https://github.com/red-hat-storage/cephci.git'
                ]]
            ])
            sharedLib = load("${env.WORKSPACE}/pipeline/vars/common.groovy")
            sharedLib.prepareNode()
        }
    }

    stage('Validate Argument') {
		ciMessage = "${params.CI_MESSAGE}" ?: ""

		if (! ciMessage?.trim() ) {
		    def defaultJsonDir = "/ceph/cephci-jenkins/latest-rhceph-container-info"
		    def composeFile = "${defaultJsonDir}/RHCEPH-${defaultRHCSVersion}-tier1.json"

		    try {
		        def compose = readJSON file: composeFile

		        // Use only the Tier-1 stable build
		        ciMessage = writeJSON returnText: true, json: compose.pass
		    } catch (err) {
		        println err.getMessage()
		        ciMessage = null
		    }
		}

		if (! ciMessage) {
		    currentBuild.result = 'ABORTED'
		    error "Aborting due to missing build parameters."
		}

		println "Triggering Tier-2 Object jobs for compose ${ciMessage}"
	}

	timeout (unit: "HOURS", time: 72) {
	    def jobResult

	    for (job in jobs) {
	        stage ("${job}") {
	            jobResult = build ([
	                propagate: false,
	                job: job,
	                parameters: [[
	                    $class: 'StringParameterValue',
	                    name: 'CI_MESSAGE',
	                    value: "${ciMessage}"
	                ]]
	            ])
	        }
	        testResults[job] = jobResult.result

	        // mark stage has failed based on the job result
	        catchError (buildResult: 'SUCCESS', stageResult: 'FAILURE') {
	            if (jobResult.result != 'SUCCESS') {
	                sh "exit 1"
	            }
	        }
	    }
	}

	stage('Publish Results') {
	    def ciValues = sharedLib.fetchComposeInfo(ciMessage)
        def rhcsVersion = ciValues.composeId.substring(7,17).toLowerCase()

        withEnv([
            "rhcephVersion=${rhcsVersion}",
            "composeId=${ciValues["composeId"]}",
            "composeUrl=${ciValues["composeUrl"]}",
            "repository=${ciValues["repository"]}"
        ]) {
            sharedLib.sendGChatNotification("Tier-2-Object")
            sharedLib.sendEMail("Tier-2-Object", testResults, false)
        }
	}

}
