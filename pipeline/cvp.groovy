/*
    Pipeline script for executing CVP test suites
*/
// Global variables section

def nodeName = "centos-7"
def tierLevel = "tier-0"
def testStages = [:]
def testResults = [:]
def cliArgs = "--build cvp"
def sharedLib
def jobStatus

def sendCVPUMBMessage(def ciMsg, def status) {
    /*
        Trigger a UMB message for successful cvp test completion
    */
    def msgMap = [
        "category": "RHCEPH CVP",
        "status": "${status}",
        "ci": [
            "url": "${env.JENKINS_URL}",
            "team": "RH Ceph QE",
            "email": "cephci@redhat.com",
            "name": "RH CEPH"
        ],
        "run": [
            "url": "${env.JENKINS_URL}/job/${env.JOB_NAME}/${env.BUILD_NUMBER}/",
            "log": "${env.JENKINS_URL}/job/${env.JOB_NAME}/${env.BUILD_NUMBER}/console"
        ],
        "system": [
            "provider": "openstack",
            "os": "rhel"
        ],
        "artifact": [
            "nvr": "${ciMsg.artifact.nvr}",
            "scratch": "false",
            "component": "${ciMsg.artifact.component}",
            "type": "brew-build",
            "id": "${ciMsg.artifact.id}",
            "issuer": "RHCEPH QE"
        ],
        "contact": [
            "name": "Downstream Ceph QE",
            "email": "ceph-qe@redhat.com"
        ],
        "test": [
            "type": "tier-0",
            "category": "validation",
            "result": "${status}",
            "namespace": "rhceph.cvp.tier0.stage"
        ],
        "generated_at": "${env.BUILD_ID}",
        "pipeline": "rhceph-cvp",
        "type": "default",
        "namespace": "rhceph-cvp-test",
        "version": "1.0.0"
    ]

    def msgContent = writeJSON returnText: true, json: msgMap
    sendCIMessage ([
        providerName: 'Red Hat UMB',
        overrides: [topic: 'VirtualTopic.eng.ci.brew-build.test.complete'],
        messageContent: "${msgContent}",
        messageProperties: "type=application/json",
        messageType: "Custom",
        failOnError: true
    ])
}

// Pipeline script entry point

node(nodeName) {

    timeout(unit: "MINUTES", time: 30) {
        stage('Install prereq') {
            checkout([
                $class: 'GitSCM',
                branches: [[name: 'origin/master']],
                doGenerateSubmoduleConfigurations: false,
                extensions: [
                    [
                        $class: 'CloneOption',
                        shallow: true,
                        noTags: true,
                        reference: '',
                        depth: 1
                    ],
                    [$class: 'CleanBeforeCheckout'],
                ],
                submoduleCfg: [],
                userRemoteConfigs: [[
                    url: 'https://github.com/red-hat-storage/cephci.git'
                ]]
            ])
            sharedLib = load("${env.WORKSPACE}/pipeline/vars/lib.groovy")
            sharedLib.prepareNode()
        }
    }

    stage("Prepare Tier-0 suite") {
        def ciMessageMap = sharedLib.getCIMessageMap()
        def versions = sharedLib.fetchMajorMinorOSVersion('cvp')
        def releaseContent = sharedLib.readFromReleaseFile(
            versions.major_version, versions.minor_version
        )
        releaseContent.cvp.repository = ciMessageMap.registry_url
        releaseContent.cvp["tag-name"] = ciMessageMap.image_tag
        def writeToFile = sharedLib.writeToReleaseFile(
            versions.major_version, versions.minor_version, releaseContent
        )
        testStages = sharedLib.fetchStages(cliArgs, tierLevel, testResults)

        currentBuild.description = ciMessageMap.artifact.nvr
    }

    parallel testStages

    stage('Publish Results') {
        def status = 'PASSED'
        if ("FAIL" in sharedLib.fetchStageStatus(testResults)) {
           status = 'FAILED'
        }
        def ciMsg = sharedLib.getCIMessageMap()
        sendCVPUMBMessage(ciMsg, status)
    }
}
