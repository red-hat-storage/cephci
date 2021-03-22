#!/usr/bin/env groovy
/*
    Common groovy methods that can be reused by the pipeline jobs.
*/

import groovy.json.JsonSlurper

def prepareNode() {
    /*
        Installs the required packages needed by the Jenkins node to
        run and execute the cephci test suites.
    */
    sh(script: "bash ${env.WORKSPACE}/pipeline/vars/node_bootstrap.bash")
}

def runTestSuite() {
    /*
        Execute the test suite by processing CI_MESSAGE for container
        information along with the environment variables. The required
        variables are
            - sutVMConf
            - sutConf
            - testSuite
            - addnArgs
    */
    println "Begin Test Suite execution"

    // generate random instance name
    def sec = sh(script: "echo \$(date +%s)", returnStdout: true).trim()
    def instanceName = "psi${sec}"

    env.rhcephVersion = "5.0-rhel-8"

    // Build the CLI options
    def cmd = "${env.WORKSPACE}/.venv/bin/python run.py"
    cmd += " --osp-cred ${env.HOME}/osp-cred-ci-2.yaml"
    cmd += " --report-portal"
    cmd += " --instances-name ${instanceName}"

    // Append test suite specific
    cmd += " --inventory ${env.sutVMConf}"
    cmd += " --global-conf ${env.sutConf}"
    cmd += " --suite ${env.testSuite}"

    // Processing CI_MESSAGE parameter, it can be empty
    def ciMessage = "${params.CI_MESSAGE}" ?: ""
    if (ciMessage?.trim()) {
        // Process the CI Message
        def jsonParser = new JsonSlurper()
        def jsonCIMsg = jsonParser.parseText("${params.CI_MESSAGE}")

        env.composeId = jsonCIMsg.compose_id
        def composeUrl = jsonCIMsg.compose_url

        def (dockerDTR, dockerImage1, dockerImage2Tag) = (jsonCIMsg.repository).split('/')
        def (dockerImage2, dockerTag) = dockerImage2Tag.split(':')
        def dockerImage = "${dockerImage1}/${dockerImage2}"

        // get rhbuild value from RHCEPH-5.0-RHEL-8.yyyymmdd.ci.x
        env.rhcephVersion = env.composeId.substring(7,17).toLowerCase()

        cmd += " --docker-registry ${dockerDTR}"
        cmd += " --docker-image ${dockerImage}"
        cmd += " --docker-tag ${dockerTag}"

        cmd += " --rhbuild ${env.rhcephVersion}"
        cmd += " --rhs-ceph-repo ${composeUrl}"
        cmd += " --insecure-registry"
        cmd += " --ignore-latest-container"

    } else {
        cmd += " --rhbuild ${env.rhcephVersion}"
    }

    // Check for additional arguments
    def addnArgFlag = "${env.addnArgs}" ?: ""
    if (addnArgFlag?.trim()) {
        cmd += " ${env.addnArgs}"
    }

    // test suite execution
    def rc = 0
    try {
        rc = sh(script: "PYTHONUNBUFFERED=1 ${cmd}", returnStatus: true)
    } catch(Exception ex) {
        rc = 1
        echo "Encountered an error"
    }

    // Forcing cleanup
    try {
        cleanup_cmd = "PYTHONUNBUFFERED=1 ${env.WORKSPACE}/.venv/bin/python"
        cleanup_cmd += " run.py --cleanup ${instanceName}"
        cleanup_cmd += " --osp-cred ${env.HOME}/osp-cred-ci-2.yaml"
        cleanup_cmd += " --log-level debug"

        sh(script: "${cleanup_cmd}")
    } catch(Exception ex) {
        echo "WARNING: Encountered an error during cleanup."
        echo "Please manually verify the test artifacts are removed."
    }

    if (rc != 0) {
        error("Test execution has failed.")
    }

}

def sendEMail(def subjectPrefix) {
    /*
        Send an email notification.
    */
    emailext(
        subject: "${subjectPrefix} test suite execution summary of ${env.composeId}",
        body: "Console logs are available at ${env.BUILD_URL}/console",
        from: "cephci@redhat.com",
        to: "cephci@redhat.com"
    )
}

def postLatestCompose() {
    /*
        Store the latest compose in ressi for QE usage.
    */
    def msgPath = "/ceph/cephci-jenkins/latest-rhceph-container-info/latest-RHCEPH-${env.rhcephVersion}.json"
    def ciMsgFlag = "${params.CI_MESSAGE}" ?: ""
    if (ciMsgFlag?.trim()) {
        sh(script: "echo ${params.CI_MESSAGE} > ${msgPath}")
    }
}

return this;

