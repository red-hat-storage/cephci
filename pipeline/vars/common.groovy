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

def getCLIArgsFromMessage(){
    /*
        Returns the arguments required for CLI after processing the CI message
    */
    env.rhcephVersion = "5.0-rhel-8"

    // Processing CI_MESSAGE parameter, it can be empty
    def ciMessage = "${params.CI_MESSAGE}" ?: ""
    println "ciMessage : " + ciMessage
    def cmd = ""

    if (ciMessage?.trim()) {
        // Process the CI Message
        def jsonParser = new JsonSlurper()
        def jsonCIMsg = jsonParser.parseText("${params.CI_MESSAGE}")

        env.composeId = jsonCIMsg.compose_id
        def composeUrl = jsonCIMsg.compose_url

        // get rhbuild value from RHCEPH-5.0-RHEL-8.yyyymmdd.ci.x
        env.rhcephVersion = env.composeId.substring(7,17).toLowerCase()
        // get rhbuild value based on OS version
        if ("${env.osVersion}" == 'RHEL-7'){
            env.rhcephVersion = env.rhcephVersion.substring(0,env.rhcephVersion.length() - 1) + '7'
            cmd += " --rhs-ceph-repo ${composeUrl}"
            cmd += " --ignore-latest-container"
        }
        else{
            cmd += " --rhs-ceph-repo ${composeUrl}"
            cmd += " --ignore-latest-container"
        }

        if(!env.containerized || (env.containerized && "${env.containerized}" == "true")){
            def (dockerDTR, dockerImage1, dockerImage2Tag) = (jsonCIMsg.repository).split('/')
            def (dockerImage2, dockerTag) = dockerImage2Tag.split(':')
            def dockerImage = "${dockerImage1}/${dockerImage2}"
            cmd += " --docker-registry ${dockerDTR}"
            cmd += " --docker-image ${dockerImage}"
            cmd += " --docker-tag ${dockerTag}"
            if ("${dockerDTR}".indexOf('registry-proxy')>=0){
                cmd += " --insecure-registry"
            }
        }

        cmd += " --rhbuild ${env.rhcephVersion}"

    } else {
        cmd += " --rhbuild ${env.rhcephVersion}"
    }

    return cmd
}

def executeTest(def cmd){
   /*
        Executes the cephci suite using the CLI input given
   */
   def rc = 0
    try {
        rc = sh(script: "PYTHONUNBUFFERED=1 ${cmd}", returnStatus: true)
    } catch(Exception ex) {
        rc = 1
        echo "Encountered an error"
    }
    return rc
}

def cleanUp(def instanceName){
   /*
       Destroys the created instances and volumes with the given instanceName from rhos-d
   */
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
    def randString = sh(script: "cat /dev/urandom | tr -cd 'a-f0-9' | head -c 5", returnStdout: true).trim()
    def instanceName = "psi${randString}"

    env.rhcephVersion = "5.0-rhel-8"

    // Build the CLI options
    def cmd = "${env.WORKSPACE}/.venv/bin/python run.py"
    cmd += " --osp-cred ${env.HOME}/osp-cred-ci-2.yaml"
    cmd += " --report-portal"
    cmd += " --instances-name ${instanceName}"

    // Append test suite specific
    cmd += " --global-conf ${env.sutConf}"
    cmd += " --suite ${env.testSuite}"
    cmd += " --inventory ${env.sutVMConf}"

    // Processing CI_MESSAGE parameter, it can be empty
    cmd += getCLIArgsFromMessage()

    // Check for additional arguments
    def addnArgFlag = "${env.addnArgs}" ?: ""
    if (addnArgFlag?.trim()) {
        cmd += " ${env.addnArgs}"
    }

    // test suite execution
    def rc = executeTest(cmd)

    // Forcing cleanup
    cleanUp(instanceName)

    return rc
}

def sendEMail(def subjectPrefix,def test_results) {
    /*
        Send an email notification.
    */
    def body = readFile(file: "pipeline/vars/emailable-report.html")
    body += "<body><u><h3>Test Summary</h3></u><br />"
    body += "<p>Logs are available at ${env.BUILD_URL}</p><br />"
    body += "<table><tr><th>Test Suite</th><th>Result</th>"
    for (test in test_results){
    def res
    if (test.value == 0){res = "PASS"} else {res = "FAIL"}
    body += "<tr><td>${test.key}</td><td>${res}</td></tr>"
    }
    body +="</table> </body> </html>"
    def to_list
    if (1 in test_results.values()){to_list = "cephci@redhat.com"}else{to_list = "ceph-qe-list@redhat.com"}
    emailext(
        mimeType: 'text/html',
        subject: "${subjectPrefix} test suite execution summary of ${env.composeId}",
        body: "${body}",
        from: "cephci@redhat.com",
        to: "${to_list}"
    )
}

def postLatestCompose() {
    /*
        Store the latest compose in ressi for QE usage.
    */
    def latestJson = "/ceph/cephci-jenkins/latest-rhceph-container-info/latest-RHCEPH-${env.rhcephVersion}.json"
    def ciMsgFlag = "${params.CI_MESSAGE}" ?: ""

    if (ciMsgFlag?.trim()) {
        sh """
            echo '${params.CI_MESSAGE}' > ${latestJson}
        """
    }
}

return this;
