/*
    Pipeline script for executing CVP test suites
*/
// Global variables section

def nodeName = "centos-7"
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
             "issuer": "Unknown issuer"
        ],
        "type": "default",
        "namespace": "rhceph-cvp-test",
        "version": "0.1.0"
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

    stage("Execute Tier-0 suite") {
        def ciMessageMap = readJSON text: "${params.CI_MESSAGE}"
        ciMessageMap['CVP'] = true
        println "ciMessage : " + ciMessageMap
        def rhcephMajorVersion = ciMessageMap.artifact.brew_build_target.substring(5,6)
        def jobName = "rhceph-${rhcephMajorVersion}-tier-0"
        def ciMessage = writeJSON returnText: true, json: ciMessageMap

        job = build ([
            wait: true,
            propagate: true,
            job: jobName,
            parameters: [string(name: 'CI_MESSAGE', value: ciMessage)]
        ])
        jobStatus = job.result
    }

    stage('Publish Results') {
        def status = 'PASSED'
        if (jobStatus != 'SUCCESS') {
           status = 'FAILED'
        }
        def ciMsg = sharedLib.getCIMessageMap()
        sendCVPUMBMessage(ciMsg, status)
    }
}
