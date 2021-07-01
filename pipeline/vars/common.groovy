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

def getCLIArgsFromMessage() {
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
        if (! "${env.composeUrl}" ) {
            env.composeUrl = jsonCIMsg.compose_url
        }

        // get rhbuild value from RHCEPH-5.0-RHEL-8.yyyymmdd.ci.x
        env.rhcephVersion = env.composeId.substring(7,17).toLowerCase()

        // get rhbuild value based on OS version
        if ("${env.osVersion}" == 'RHEL-7') {
            env.rhcephVersion = env.rhcephVersion.substring(0,env.rhcephVersion.length() - 1) + '7'
            cmd += " --rhs-ceph-repo ${env.composeUrl}"
            cmd += " --ignore-latest-container"
        } else {
            cmd += " --rhs-ceph-repo ${env.composeUrl}"
            cmd += " --ignore-latest-container"
        }

        if (!env.containerized || (env.containerized && "${env.containerized}" == "true")) {
            def (dockerDTR, dockerImage1, dockerImage2Tag) = (jsonCIMsg.repository).split('/')
            def (dockerImage2, dockerTag) = dockerImage2Tag.split(':')
            def dockerImage = "${dockerImage1}/${dockerImage2}"
            env.repository = jsonCIMsg.repository
            cmd += " --docker-registry ${dockerDTR}"
            cmd += " --docker-image ${dockerImage}"
            cmd += " --docker-tag ${dockerTag}"
            if ("${dockerDTR}".indexOf('registry-proxy') >= 0) {
                cmd += " --insecure-registry"
            }
        }

        cmd += " --rhbuild ${env.rhcephVersion}"

    } else {
        cmd += " --rhbuild ${env.rhcephVersion}"
    }

    return cmd
}

def cleanUp(def instanceName) {
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
        println "WARNING: Encountered an error during cleanup."
        println "Please manually verify the test artifacts are removed."
    }
}

def executeTest(def cmd, def instanceName) {
   /*
        Executes the cephci suite using the CLI input given
   */
    def rc = 0
    catchError (message: 'STAGE_FAILED', buildResult: 'FAILURE', stageResult: 'FAILURE') {
        try {
            sh(script: "PYTHONUNBUFFERED=1 ${cmd}")
        } catch(Exception err) {
            rc = 1
            println err.getMessage()
            error "Encountered an error"
        } finally {
            cleanUp(instanceName)
        }
    }

    return rc
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
    def randString = sh(
                            script: "cat /dev/urandom | tr -cd 'a-f0-9' | head -c 5",
                            returnStdout: true
                        ).trim()
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
    echo cmd
    return executeTest(cmd, instanceName)
}

def fetchEmailBodyAndReceiver(def test_results, def isStage) {
    def to_list = "ceph-qe-list@redhat.com"
    def jobStatus = "stable"
    def failureCount = 0
    
    def body = "<table>"

    if(isStage) {
        body += "<tr><th>Test Suite</th><th>Result</th>"
        for (test in test_results) {
            def res = "PASS"
            if (test.value != 0) {
                res = "FAIL"
                failureCount += 1
            } 

            body += "<tr><td>${test.key}</td><td>${res}</td></tr>"
        }
    }
    else {
        body += "<tr><th>Jenkins Job Name</th><th>Result</th>"
        for (test in test_results) {
            if (test.value != "SUCCESS") {
                failureCount += 1
            }

            body += "<tr><td>${test.key}</td><td>${test.value}</td></tr>"
        }
    }

    body +="</table> </body> </html>"

    if (failureCount > 0) {
        to_list = "cephci@redhat.com"
        jobStatus = 'unstable'
    }
    return ["to_list" : to_list, "jobStatus" : jobStatus, "body" : body]
}

def sendEMail(def subjectPrefix,def test_results,def isStage=true) {
    /*
        Send an email notification.
    */
    def body = readFile(file: "pipeline/vars/emailable-report.html")
    body += "<body><u><h3>Test Summary</h3></u><br />"
    body += "<p>Logs are available at ${env.BUILD_URL}</p><br />"

    def params = fetchEmailBodyAndReceiver(test_results, isStage)
    body += params["body"]

    def to_list = params["to_list"]
    def jobStatus = params["jobStatus"]

    emailext(
        mimeType: 'text/html',
        subject: "${env.composeId} build is ${jobStatus} at QE ${subjectPrefix} stage.",
        body: "${body}",
        from: "cephci@redhat.com",
        to: "${to_list}"
    )
}

def postLatestCompose() {
    /*
        Store the latest compose in ressi for QE usage.
    */
    def defaultFileDir = "/ceph/cephci-jenkins/latest-rhceph-container-info"
    def latestJson = "${defaultFileDir}/latest-RHCEPH-${env.rhcephVersion}.json"
    def tier0Json = "${defaultFileDir}/RHCEPH-${env.rhcephVersion}-tier0.json"
    def ciMsgFlag = "${params.CI_MESSAGE}" ?: ""

    if (ciMsgFlag?.trim()) {
        sh """
            echo '${params.CI_MESSAGE}' > ${latestJson};
            echo '${params.CI_MESSAGE}' > ${tier0Json};
        """
    }
}

def sendUMBMessage(def msgType) {
    /*
        Trigger a UMB message for successful tier completion
    */
    def msgContent = """
    {
        "BUILD_URL": "${env.BUILD_URL}",
        "CI_STATUS": "PASS",
        "COMPOSE_ID": "${env.composeId}",
        "COMPOSE_URL": "${env.composeUrl}",
        "PRODUCT": "Red Hat Ceph Storage",
        "REPOSITORY": "${env.repository}",
        "TOOL": "cephci"
    }
    """

    def msgProperties = """ BUILD_URL = ${env.BUILD_URL}
        CI_STATUS = PASS
        COMPOSE_ID = ${env.composeId}
        COMPOSE_URL = ${env.composeUrl}
        PRODUCT = Red Hat Ceph Storage
        REPOSITORY = ${env.repository}
        TOOL = cephci
    """

    def sendResult = sendCIMessage \
        providerName: 'Red Hat UMB', \
        overrides: [topic: 'VirtualTopic.qe.ci.jenkins'], \
        messageContent: "${msgContent}", \
        messageProperties: msgProperties, \
        messageType: msgType, \
        failOnError: true

    return sendResult
}

def fetchTier1Compose() {
    /*
       Fetch the compose to be tested for tier1, is there is one
    */
    def defaultFileDir = "/ceph/cephci-jenkins/latest-rhceph-container-info"
    def tier0Json = "${defaultFileDir}/RHCEPH-${env.rhcephVersion}-tier0.json"
    def tier1Json = "${defaultFileDir}/RHCEPH-${env.rhcephVersion}-tier1.json"

    def tier0FileExists = sh(returnStatus: true, script: """ls -l '${tier0Json}'""")
    if (tier0FileExists != 0) {
        println "RHCEPH-${env.rhcephVersion}-tier0.json does not exist."
        return null
    }
    def tier0Compose = sh(returnStdout: true, script: """ cat '${tier0Json}' """).trim()

    def tier1FileExists = sh(returnStatus: true, script: """ls -l '${tier1Json}'""")
    if (tier1FileExists != 0) {
        return tier0Compose
    }

    def tier1Compose = sh(returnStdout: true, script: """ cat '${tier1Json}' """).trim()
    def jsonParser = new JsonSlurper()

    def jsonCIMsgTier0 = jsonParser.parseText("${tier0Compose}")
    def tier0composeId = jsonCIMsgTier0.compose_id

    def jsonCIMsgTier1 = jsonParser.parseText("${tier1Compose}")
    def tier1composeId = jsonCIMsgTier1.latest.compose_id

    if (tier0composeId != tier1composeId) {
        return tier0Compose
    }

    println "There are no new stable build."
    return null
}

def postTier1Compose(def test_results, def composeInfo) {
    /*
        Store the latest compose in ressi for QE usage.
    */
    def defaultFileDir = "/ceph/cephci-jenkins/latest-rhceph-container-info"
    def tier1Json = "${defaultFileDir}/RHCEPH-${env.rhcephVersion}-tier1.json"
    def jsonContent = """{ "latest" : ${composeInfo}, "pass" : ${composeInfo} }"""

    if (1 in test_results.values()) {
        def tier1FileExists = sh(returnStatus: true, script: """ls -l '${tier1Json}'""")
        if ( tier1FileExists != 0) {
            jsonContent = """ {"latest" : ${composeInfo}, "pass" : ""} """
        } else {
            def jsonParser = new JsonSlurper()
            def tier1Compose = sh(
                                    returnStdout: true,
                                    script: """cat ${tier1Json}"""
                               ).trim()
            def tier1JsonContent = jsonParser.parseText("${tier1Compose}")
            jsonContent = """
                {
                    "latest": ${composeInfo},
                    "pass": ${tier1JsonContent.pass}
                }
            """
        }
    }

    sh """
        echo '${jsonContent}' > ${tier1Json};
    """

}

def fetchComposeInfo(def composeInfo) {
    /*
        Return a map after processing the passed information.
    */
    def jsonParser = new JsonSlurper()
    def jsonCIMsg = jsonParser.parseText("${composeInfo}") 

    return [
                "composeId" : jsonCIMsg.compose_id,
                "composeUrl" : jsonCIMsg.compose_url,
                "repository" : jsonCIMsg.repository
           ]
}

return this;
