/* Library module that contains methods to send email and gchat notifications. */

def sendEmail(
    def sharedLib,
    def run_type,
    def testResults,
    def artifactDetails,
    def tierLevel = null,
    def stageLevel = null,
    def version = null,
    def toList="cephci@redhat.com"
){
    /* Send an Email notification
    Arguments:
        testResults: map of the test suites and its status
            Example: testResults = {
                                        "01_deploy": {"result": "PASS",
                                                      "logdir": "report_portal_url"},
                                        "02_object": {"result": "PASS",
                                                      "logdir": "report_portal_url"},
                                   }

        artifactDetails: Map of artifact details
            Example: artifactDetails = {"composes": {"rhel-7": "composeurl1",
                                                     "rhel-8": "composeurl2"},
                                        "product": "Redhat",
                                        "version": "RHCEPH-5.0",
                                        "ceph_version": "16.2.0-117",
                                        "repository": "repositoryname",
                                        "build_url": "jenkins build url",
                                        "report_portal": "report portal url",
                                        "upstreamVersion": "quincy"}
        tierLevel:
            Example: Tier0, Tier1, CVP..

        stageLevel:
            Example: Stage-0, Stage-1..
    */
    def status = "STABLE"
    println("testResults : ${testResults}")
    println("artifactDetails : ${artifactDetails}")
    if ('FAIL' in sharedLib.fetchStageStatus(testResults)) {
        println("status in : ${status}")
        if(toList == "ceph-qe-list@redhat.com"){
            toList = "cephci@redhat.com"
        }
        status = "UNSTABLE"
    }

    println("status : ${status}")
    def subject = ""
    def artifactJson = writeJSON returnText: true, json: artifactDetails
    def testResultsJson = writeJSON returnText: true, json: testResults
    println("artifactJson : ${artifactJson}")
    def ceph_version = artifactDetails["ceph_version"]
    if (!ceph_version){
        ceph_version = artifactDetails["ceph-version"]
    }
    println("ceph_version : ${ceph_version}")
    if (!version){
        version = artifactDetails["version"]
    }
    if (run_type == "Live" || run_type == "upstream") {
        println(run_type)
        if (run_type == "Live"){
            subject = "${run_type} test report status for RHCEPH-${artifactDetails.rhcephVersion} ceph version:${ceph_version} is ${status}"
        }
        if (run_type == "upstream") {
            subject = "Upstream Ceph Version:${ceph_version}-${artifactDetails.upstreamVersion} ${stageLevel.capitalize()} Automated test execution summary"
        }
    }
    else{
        subject = "${run_type} for ${tierLevel.capitalize()} ${stageLevel.capitalize()} test report status of ${version} - ${ceph_version} is ${status}"
    }

    try {
        def cmd = "cd ${env.WORKSPACE}/pipeline/scripts/ci;"
        cmd = "${cmd} sudo ${env.WORKSPACE}/.venv/bin/python get_email_body.py"
        cmd = "${cmd} --template stage_email.jinja"
        cmd = "${cmd} --testResults '${testResultsJson}'"
        cmd = "${cmd} --testArtifacts '${artifactJson}'"

        println("email cmd")
        println("${cmd}")

        def emailBody = sh (returnStdout: true, script: cmd)
        println("emailBody : ${emailBody}")
        emailBody = readYaml text: emailBody
        body = emailBody["email_body"]

        emailext (
            mimeType: 'text/html',
            subject: "${subject}",
            body: "${body}",
            from: "cephci@redhat.com",
            to: "${toList}"
        )
    } catch(Exception exc) {
        println "Encountered a failure during email body creation."
        println exc
    }
}

def sendConsolidatedEmail(
    def run_type = "",
    def artifactDetails,
    def majorVersion,
    def minorVersion,
    def cephVersion,
    def toList="cephci@redhat.com"
    ) {
    /*
        Send a consolidated Email summary for test execution
        Arguments:
            artifactDetails: Map of artifact details
                Example: artifactDetails = {"composes": {"rhe-7": "composeurl1",
                                                         "rhel-8": "composeurl2"},
                                            "product": "Redhat",
                                            "version": "RHCEPH-5.0",
                                            "ceph_version": "16.2.0-117",
                                            "repository": "repositoryname"}
            majorVersion.minorVersion:
                Example: 4.3, 5.2, 6.0

            cephVersion:
                Example: 16.2.0-117
    */
    type = run_type.replaceAll(" ", "_")
    test_res_file = "/ceph/cephci-jenkins/results/${cephVersion}.yaml"
    println("test_res_file: ${test_res_file}")
    testArtifacts = writeJSON returnText: true, json: artifactDetails

    println("testArtifacts : ${testArtifacts}")

    def subject = "RHCEPH ${majorVersion}.${minorVersion} - ${cephVersion} ${run_type} test execution report"

    try {
        def cmd = "cd ${env.WORKSPACE}/pipeline/scripts/ci;"
        cmd = "${cmd} sudo ${env.WORKSPACE}/.venv/bin/python get_email_body.py"
        cmd = "${cmd} --template consolidated_email.jinja"
        cmd = "${cmd} --testResultsFile '${test_res_file}'"
        cmd = "${cmd} --testArtifacts '${testArtifacts}'"
        cmd = "${cmd} --runType '${type}'"

        def emailBody = sh (returnStdout: true, script: cmd)
        println("emailBody : ${emailBody}")
        emailBody = readYaml text: emailBody
        body = emailBody["email_body"]

        println("body: ${body}")

        emailext (
            mimeType: 'text/html',
            subject: "${subject}",
            body: "${body}",
            from: "cephci@redhat.com",
            to: "${toList}"
        )
    } catch(Exception exc) {
        println "Encountered a failure during email body creation."
        println exc
    }
}

def sendGChatNotification(
    def run_type,
    def testResults,
    def tierLevel,
    def stageLevel = null,
    def build_url = null,
    def rhcephVersion = null) {
    /*
        Send a GChat notification.
        Plugin used:
            googlechatnotification which allows to post build notifications to a Google
            Chat Messenger groups.
            parameter:
                url: Mandatory String parameter.
                     Single/multiple comma separated HTTP URLs or/and single/multiple
                     comma separated Credential IDs.
                message: Mandatory String parameter.
                         Notification message to be sent.
                         Notification message to be sent.
    */
    def ciMsg = getCIMessageMap()
    def status = "STABLE"
    if ('FAIL' in fetchStageStatus(testResults)) {
        status = "UNSTABLE"
    }
    if (! build_url){
        build_url = "${env.BUILD_URL}"
    }
    def msg= ""
    if (run_type == "Live"){
        msg= "${run_type} test run for RHCEPH-${rhcephVersion} is ${status}.Log:${build_url}"
    } else {
        msg= "${run_type} for ${ciMsg.artifact.nvr}:${tierLevel} ${stageLevel} is ${status}.Log:${build_url}"
    }
    googlechatnotification(url: "id:rhcephCIGChatRoom", message: msg)
}

return this;