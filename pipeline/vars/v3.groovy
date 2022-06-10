/* Library module that contains methods used across Ceph QE CI pipeline. */

import org.jsoup.Jsoup

def fetchCephVersion(def baseUrl) {
    /*
        Fetches ceph version using compose base url
    */
    baseUrl += "/compose/Tools/x86_64/os/Packages/"
    println baseUrl
    def document = Jsoup.connect(baseUrl).get().toString()
    def cephVer = document.findAll(/"ceph-common-([\w.-]+)\.([\w.-]+)"/)[0].findAll(
        /([\d]+)\.([\d]+)\.([\d]+)\-([\d]+)/
    )

    println cephVer
    if (! cephVer) {
        error "ceph version not found.."
    }

    return cephVer[0]
}

def writeToReleaseFile(
    def majorVer,
    def minorVer,
    def dataContent,
    def location="/ceph/cephci-jenkins/latest-rhceph-container-info"
    ) {
    /*
        Method write content from the release yaml file and unset the lock.
    */
    def releaseFile = "RHCEPH-${majorVer}.${minorVer}.yaml"
    writeYaml file: "${location}/${releaseFile}", data: dataContent, overwrite: true
    unSetLock(majorVer, minorVer)
}

def sendEmail(
    def run_type,
    def testResults,
    def artifactDetails,
    def tierLevel,
    def stageLevel = null,
    def toList="ceph-qe-list@redhat.com"
    ) {
    /*
        Send an Email
        Arguments:
            testResults: map of the test suites and its status
                Example: testResults = [
                                    "01_deploy": ["status": "PASS",
                                                  "log-dir": "file_path1/"],
                                    "02_object": ["status": "PASS",
                                                  "log-dir": "file_path2/"]
                                ]
            artifactDetails: Map of artifact details
                Example: artifactDetails = ["composes": ["rhe-7": "composeurl1",
                                                         "rhel-8": "composeurl2"],
                                            "product": "Redhat",
                                            "version": "RHCEPH-5.0",
                                            "ceph_version": "16.2.0-117",
                                            "repository": "repositoryname"]
            tierLevel:
                Example: Tier0, Tier1, CVP..

            stageLevel:
                Example: Stage-0, Stage-1..
    */
    def status = "STABLE"
    def body = readFile(file: "pipeline/vars/emailable-report.html")

    body += "<body>"
    body += "<h3><u>Test Summary</u></h3>"
    body += "<table>"
    body += "<tr><th>Test Suite</th><th>Result</th></tr>"

    testResults.each{k,v->
        def test_name = k.replace("-", " ")
        body += "<tr><td>${test_name.capitalize()}</td><td>${v['status']}</td></tr>"
    }

    body += "</table><br />"
    body += "<h3><u>Test Artifacts</u></h3>"
    body += "<table>"

    if (artifactDetails.product) {
        body += "<tr><td>Product</td><td>${artifactDetails.product}</td></tr>"
    }
    if (artifactDetails.version) {
        body += "<tr><td>Version</td><td>${artifactDetails.version}</td></tr>"
    }
    if (artifactDetails.ceph_version) {
        body += "<tr><td>Ceph Version </td><td>${artifactDetails.ceph_version}</td></tr>"
    }
    if (artifactDetails.composes) {
        body += "<tr><td>Composes</td><td>${artifactDetails.composes}</td></tr>"
    }
    if (artifactDetails.repository) {
        body += "<tr><td>Container Image</td><td>${artifactDetails.repository}</td></tr>"
    }
    if (artifactDetails.log) {
        body += "<tr><td>Log</td><td>${artifactDetails.log}</td></tr>"
    } else {
        body += "<tr><td>Log</td><td>${env.BUILD_URL}</td></tr>"
    }
    if ( artifactDetails.buildArtifacts ){
        body += "</table><h3><u>Build Artifacts</u></h3><table>"

        if (artifactDetails.buildArtifacts.composes) {
            body += "<tr><td>Build Composes</td><td>${artifactDetails.buildArtifacts.composes}</td></tr>"
        }
        if (artifactDetails.buildArtifacts.repository) {
            body += "<tr><td>Container Image</td><td>${artifactDetails.buildArtifacts.repository}</td></tr>"
        }
    }

    body += "</table><br /></body></html>"

    if ('FAIL' in fetchStageStatus(testResults)) {
        if(toList == "ceph-qe-list@redhat.com"){
            toList = "cephci@redhat.com"
        }
        status = "UNSTABLE"
    }

    def subject = "${run_type} for ${tierLevel.capitalize()} ${stageLevel.capitalize()} test report status of ${artifactDetails.version} - ${artifactDetails.ceph_version} is ${status}"

    emailext (
        mimeType: 'text/html',
        subject: "${subject}",
        body: "${body}",
        from: "cephci@redhat.com",
        to: "${toList}"
    )
}

def sendGChatNotification(def run_type, def testResults, def tierLevel, def stageLevel= null, def build_url= null) {
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
    def msg= "${run_type} for ${ciMsg.artifact.nvr}:${tierLevel} ${stageLevel} is ${status}.Log:${build_url}"
    googlechatnotification(url: "id:rhcephCIGChatRoom", message: msg)
}

def generateRandomString() {
    return sh(
        script: "cat /dev/urandom | tr -cd 'a-z0-9' | head -c 5",
        returnStdout: true
    ).trim()
}

def buildArtifactsDetails(def content, def ciMsgMap, def phase) {
    /* Return artifacts details using release content */
    return [
        "composes": content[phase]["composes"],
        "product": "Red Hat Ceph Storage",
        "version": ciMsgMap,
        "ceph_version": content[phase]["ceph-version"],
        "repository": content[phase]["repository"]
    ]
}

def uploadCompose(def rhBuild, def cephVersion, def baseUrl) {
    /*
        This method is a wrapper around upload_compose.py which passes the given
        arguments to the upload_compose script. It supports

        Args:
            rhBuild     RHCS Build in the format ceph-<major>.<minor>-<platform>
            cephVersion The ceph version
            baseUrl     The compose base URL
    */
    try {
        def cpFile = "sudo cp ${env.HOME}/.cephci.yaml /root/"
        def cmd = "sudo ${env.WORKSPACE}/.venv/bin/python"
        def scriptFile = "pipeline/scripts/ci/upload_compose.py"
        def args = "${rhBuild} ${cephVersion} ${baseUrl}"

        sh script: "${cpFile} && ${cmd} ${scriptFile} ${args}"
    } catch(Exception exc) {
        println "Encountered a failure during compose upload."
        println exc
    }
}

def uploadResults(def objKey, def dirName, def bucketName="qe-ci-reports") {
    /*
        Uploads the given directory to COS using cos_cli.py module

        example: python3 cos_cli.py upload xunit.xml dirName/objKey bucket-1

        Args:
            objKey      Prefix to be used for uploaded objects.
            dirName     Name of the directory to be uploaded.
            bucketName  Name of the bucket to which the contents are uploaded.
    */
    try {
        def cpFile = "sudo cp ${env.HOME}/.cephci.yaml /root/"
        def cmd = "sudo ${env.WORKSPACE}/.venv/bin/python"
        def scriptFile = "pipeline/scripts/ci/cos_cli.py"
        def args = "upload_directory ${dirName} ${objKey} ${bucketName}"

        sh script: "${cpFile} && ${cmd} ${scriptFile} ${args}"
    } catch(Exception exc) {
        println exc
        error "Encountered a failure during uploading of results."
    }
}

def writeToRecipeFile(
    def buildType, def rhcephVersion, def dataPhase, def currentStage, def status, def infra="10.245.4.89"
    ) {
    /*
        Method to update content to the recipe file
    */
    def result = "results"
    def recipeFile = "/data/site/recipe/${rhcephVersion}.yaml"
    recipeFileExist(rhcephVersion, recipeFile, infra)
    if("${buildType}" == "latest" || "${currentStage}" == "stage-1"){
        sh "ssh $infra \"yq eval -i '.$dataPhase = .latest' $recipeFile\""
        sh "ssh $infra \"yq e -i '.$dataPhase.$result.$currentStage = \\\"$status\\\"' $recipeFile\""
    }
    else{
        sh "ssh $infra \"yq e -i '.$dataPhase.$result.$currentStage = \\\"$status\\\"' $recipeFile\""
    }
}

def executeTestSuite(
    def cliArgs, def cleanup_on_success=true, def cleanup_on_failure=true
    ) {
    /*
        This method executes a single test suite and also performs cleanup of the VM.

        Args:
            cliArgs - argument to be passed to run.py
    */
    def rc = "PASS"
    def randString = sh(
        script: "cat /dev/urandom | tr -cd 'a-z0-9' | head -c 5",
        returnStdout: true
    ).trim()
    def vmPrefix = "ci-${randString}"
    def baseCmd = ".venv/bin/python run.py --log-level DEBUG"
    baseCmd += " --osp-cred ${env.HOME}/osp-cred-ci-2.yaml"

    try {
        sh (script: "${baseCmd} --instances-name ${vmPrefix} ${cliArgs}")
    } catch (err) {
        rc = "FAIL"
        println err
        currentBuild.result = 'FAILURE'
    } finally {
        if ((cleanup_on_success && cleanup_on_failure) ||
            (cleanup_on_success && rc == 'PASS') ||
            (cleanup_on_failure && rc == 'FAIL'))
        {
            sh(script: "${baseCmd} --cleanup ${vmPrefix}")
        } else {
            println "Not performing cleanup of cluster."
        }
    }

    return [ "result": rc, "instances-name": vmPrefix]
}

def configureRpPreProc(
    def rpPreprocFile=".rp_preproc_conf.yaml",
    def location="/ceph/cephci-jenkins"
    ) {
    /*
        This definition is to configure rclone to access IBM-COS
    */
    try {
        def tmpString = generateRandomString()
        def rp_preproc_dir = "${env.WORKSPACE}/rp_preproc-${tmpString}"

        sh script: "mkdir -p ${rp_preproc_dir}/payload"
        credsRpProc = yamlToMap(rpPreprocFile, location)

        return [rp_preproc_dir, credsRpProc]
    } catch(Exception err) {
        println err.getMessage()
        error "Encountered an error"
    }
}

def uploadTestResults(def sourceDir, def credPreproc, def runProperties) {
    /*
        upload Xunit Xml file to report portal and polarion

        - move xml file to ${preprocDir}/payload/results
        - configure rp_preproc launch
        - upload xml file to report portal using rp_preproc
        - rclone delete xml file
        - upload test run results to polarion

        Args:
            sourceDir       Working directory containing payload
            credPreproc     rp_preproc creds
            runProperties   Metadata information about the launch.
    */
    def msgMap = getCIMessageMap()
    def credFile = "${sourceDir}/config.json"

    // Configure rp_preproc launch
    def launchConfig = [
        "name": "${runProperties['version']} - ${runProperties['stage']}",
        "description": "Test executed on ${runProperties['date']}",
        "attributes": [
            "ceph_version": runProperties["ceph_version"],
            "rhcs": runProperties["version"].split('-')[1],
            "tier": runProperties["stage"],
        ]
    ]
    credPreproc["reportportal"]["launch"] = launchConfig
    writeJSON file: credFile, json: credPreproc

    // Upload xml file to report portal
    sh(script: ".venv/bin/python utility/rp_client.py -c ${credFile} -d ${sourceDir}/payload")

    // Upload test result to polarion using xUnit Xml file
    withCredentials([
        usernamePassword(
            credentialsId: 'psi-ceph-jenkins',
            usernameVariable: 'OSPUSER',
            passwordVariable: 'OSPCRED'
        )
    ]){
        def polarionUrl = "https://polarion.engineering.redhat.com/polarion/import/xunit"
        def xmlFiles = sh (returnStdout: true, script: "ls ${sourceDir}/payload/results/*.xml | cat")
        if (! xmlFiles ){
            return
        }
        def cmdArgs = "curl -k -u '${OSPUSER}:${OSPCRED}' -X POST -F file=@FILE_NAME ${polarionUrl}"
        def xmlFileNames = xmlFiles.split("\\n")
        for (filePath in xmlFileNames) {
            def localCmd = cmdArgs.replace("FILE_NAME", filePath)
            sh script: "${localCmd}"
        }
    }
}

def fetchStageStatus(def testResults) {
    /*
        This method is to return list stage status(es).

        Args:
            testResults - test results of test stages in a map
    */
    def stageStatus = []
    testResults.each { key, value -> stageStatus.add(value["status"]) }

    return stageStatus
}

def prepareNode(def listener=0, def project="ceph-jenkins") {
    /*
        Installs the required packages needed by the Jenkins node to
        run and execute the cephci test suites.
    */
    withCredentials([
        usernamePassword(
            credentialsId: 'psi-ceph-jenkins',
            usernameVariable: 'OSPUSER',
            passwordVariable: 'OSPCRED'
        )
    ]) {
        def ospMap = [
            "globals": [
                "openstack-credentials": [
                    "username": OSPUSER,
                    "password": OSPCRED,
                    "auth-url": "https://rhos-d.infra.prod.upshift.rdu2.redhat.com:13000",
                    "auth-version": "3.x_password",
                    "tenant-name": project,
                    "service-region": "regionOne",
                    "domain": "redhat.com",
                    "tenant-domain-id": "62cf1b5ec006489db99e2b0ebfb55f57"
                ]
            ]
        ]
        writeYaml file: "${env.HOME}/osp-cred-ci-2.yaml", data: ospMap, overwrite: true
    }

    sh (script: "bash ${env.WORKSPACE}/pipeline/vars/node_bootstrap.bash $listener")
}

def prepareIbmNode() {
    /*
        Installs the required packages needed by the IBM Jenkins node to
        run and execute the cephci test suites. IbmDetails
    */
    withCredentials([
        file(credentialsId: 'cephCIIBMCToken', variable: 'ibmDetails'),
        file(credentialsId: 'cephCIConf', variable: 'cephciDetails')
        ]) {

        def ibmFileExists = sh(
            returnStatus: true,
            script: "ls -l ${env.HOME}/osp-cred-ci-2.yaml"
            )

        if ( ibmFileExists != 0 ) {
            println "${env.HOME}/osp-cred-ci-2.yaml does not exist. creating it"
            writeFile file: "${env.HOME}/osp-cred-ci-2.yaml", text: readFile(ibmDetails)
        }
        def cephciFileExists = sh(
            returnStatus: true,
            script: "ls -l ${env.HOME}/.cephci.yaml"
            )
        if ( cephciFileExists != 0 ) {
            println "${env.HOME}/.cephci.yaml does not exist. creating it"
            writeFile file: "${env.HOME}/.cephci.yaml", text: readFile(cephciDetails)
        }
        sh "rm -rf .venv"
        sh "python3 -m venv .venv"
        sh ".venv/bin/python -m pip install --upgrade pip"
        sh ".venv/bin/python -m pip install -r requirements.txt"
        println "Done preparing the node"
    }
}

def getCIMessageMap() {
    /*
        Return the CI_MESSAGE map
    */
    def ciMessage = "${params.CI_MESSAGE}" ?: ""
    if (! ciMessage?.trim() ) {
        return [:]
    }
    def compose = readJSON text: "${params.CI_MESSAGE}"
    return compose
}

def readFromRecipeFile(def rhcephVersion, def infra="10.245.4.89") {
    /*
        Method to read content from the recipe file.
    */
    def recipeFile = "/data/site/recipe/${rhcephVersion}.yaml"
    recipeFileExist(rhcephVersion, recipeFile, infra)

    def content = sh(
        script: "ssh ${infra} \"yq e '.' ${recipeFile}\"", returnStdout: true
    )
    def contentMap = readYaml text: content

    return contentMap
}

def recipeFileExist(def rhcephVersion, def recipeFile, def infra) {
    /*
        Method to check existence of recipe file.
    */
    def fileExist = sh(
        returnStatus: true,
        script: "ssh $infra \"sudo ls -l $recipeFile\""
        )
    if (fileExist != 0) {
        error "Recipe file ${rhcephVersion}.yaml does not exist.."
    }
}

def yamlToMap(
    def yamlFile,
    def location="/ceph/cephci-jenkins/latest-rhceph-container-info"
    ) {
    /*
        Read the yaml file and returns a map object
    */
    def yamlFileExists = sh (
        returnStatus: true, script: "ls -l ${location}/${yamlFile}"
    )
    if (yamlFileExists != 0) {
        println "File ${location}/${yamlFile} does not exist."
        return [:]
    }
    def props = readYaml file: "${location}/${yamlFile}"
    return props
}

def readFromReleaseFile(
    def majorVer,
    def minorVer,
    def lockFlag=true,
    def location="/ceph/cephci-jenkins/latest-rhceph-container-info"
    ) {
    /*
        Method to set lock and read content from the release yaml file.
    */
    def releaseFile = "RHCEPH-${majorVer}.${minorVer}.yaml"
    if (lockFlag) {
        setLock(majorVer, minorVer)
    }
    def dataContent = yamlToMap(releaseFile, location)
    println "content of release file is: ${dataContent}"

    return dataContent
}

def setLock(def majorVer, def minorVer) {
    /*
        create a lock file
    */
    def defaultFileDir = "/ceph/cephci-jenkins/latest-rhceph-container-info"
    def lockFile = "${defaultFileDir}/RHCEPH-${majorVer}.${minorVer}.lock"
    def lockFileExists = sh (returnStatus: true, script: "ls -l ${lockFile}")
    if (lockFileExists != 0) {
        println "RHCEPH-${majorVer}.${minorVer}.lock does not exist. creating it"
        sh(script: "touch ${lockFile}")
        return
    }
    def startTime = System.currentTimeMillis()
    while( (System.currentTimeMillis()-startTime) < 600000 ) {
        sleep(2)
        lockFilePresent = sh (returnStatus: true, script: "ls -l ${lockFile}")
        if (lockFilePresent != 0) {
            sh(script: "touch ${lockFile}")
            return
        }
    }
    error "Lock file: RHCEPH-${majorVer}.${minorVer}.lock already exist.can not create lock file"
}

def unSetLock(def majorVer, def minorVer) {
    /* Unset a lock file */
    def defaultFileDir = "/ceph/cephci-jenkins/latest-rhceph-container-info"
    def lockFile = "${defaultFileDir}/RHCEPH-${majorVer}.${minorVer}.lock"
    sh(script: "rm -f ${lockFile}")
}

def compareCephVersion(def oldCephVer, def newCephVer) {
    /*
        compares new and old ceph versions.
        returns 0 if equal
        returns 1 if new ceph version is greater than old ceph version
        returns -1 if new ceph version is lesser than old ceph version

        example for ceph version: 16.2.0-117, 14.2.11-190
    */

    if (newCephVer == oldCephVer) {
        return 0
    }

    def oldVer = oldCephVer.split("\\.|-").collect { it.toInteger() }
    def newVer = newCephVer.split("\\.|-").collect { it.toInteger() }

    if (newVer[0] > oldVer[0]) {
        return 1
    }
    else if (newVer[0] < oldVer[0]) {
        return -1
    }

    if (newVer[1] > oldVer[1]) {
        return 1
    }
    else if (newVer[1] < oldVer[1]) {
        return -1
    }

    if (newVer[2] > oldVer[2]) {
        return 1
    }
    else if (newVer[2] < oldVer[2]) {
        return -1
    }

    if (newVer[3] > oldVer[3]) {
        return 1
    }
    else if (newVer[3] < oldVer[3]) {
        return -1
    }

}

def getRHCSVersionFromArtifactsNvr() {
    /*
        Returns the RHCEPH version from the compose ID in CI_MESSAGE.
    */
    def compose = getCIMessageMap()
    def (major_version, minor_version) = compose.artifact.nvr.substring(7,).tokenize(".")
    return ["major_version": major_version, "minor_version": minor_version]
}

def fetchMajorMinorOSVersion(def buildType) {
    /*
        Returns a tuple Major, Minor and platform of the build.

        buildType:  type of the build. Supported types are unsigned-compose,
                    unsigned-container-image, cvp, signed-compose,
                    signed-container-image

        Returns RH-CEPH major version, minor version and OS platform based on buildType

    */
    def cimsg = getCIMessageMap()
    def majorVer
    def minorVer
    def platform

    if (buildType == 'unsigned-compose' || buildType == 'unsigned-container-image') {
        majorVer = cimsg.compose_id.substring(7,8)
        minorVer = cimsg.compose_id.substring(9,10)
        platform = cimsg.compose_id.substring(11,17).toLowerCase()
    }
    if (buildType == 'cvp') {
        majorVer = cimsg.artifact.brew_build_target.substring(5,6)
        minorVer = cimsg.artifact.brew_build_target.substring(7,8)
        platform = cimsg.artifact.brew_build_target.substring(9,15).toLowerCase()
    }
    if (buildType == 'signed-compose') {
        majorVer = cimsg["compose-id"].substring(7,8)
        minorVer = cimsg["compose-id"].substring(9,10)
        platform = cimsg["compose-id"].substring(11,17).toLowerCase()
    }
    if (buildType == 'signed-container-image') {
        majorVer = cimsg.tag.name.substring(5,6)
        minorVer = cimsg.tag.name.substring(7,8)
        platform = cimsg.tag.name.substring(9,15).toLowerCase()
    }
    if (buildType == 'released') {
        majorVer = cimsg.tag.name.substring(5,6)
        minorVer = cimsg.tag.name.substring(7,8)
        platform = cimsg.tag.name.substring(9,15).toLowerCase()
    }
    if (majorVer && minorVer && platform) {
        return ["major_version":majorVer, "minor_version":minorVer, "platform":platform]
    }
    error "Required values are not obtained.."
}

def executeTestScript(def script) {
   /* Executes the test script */
    def rc = "PASS"
    def executeCLI = script["execute_cli"]
    def cleanupCLI = script["cleanup_cli"]

    catchError(
        message: 'STAGE_FAILED',
        buildResult: 'FAILURE',
        stageResult: 'FAILURE'
        ) {
        try {
            sh (script: "${env.WORKSPACE}/${executeCLI}")
        } catch(Exception err) {
            rc = "FAIL"
            println err.getMessage()
            error "Encountered an error"
        } finally {
            sh(script: "${env.WORKSPACE}/${cleanupCLI}")
        }
    }
    println "exit status: ${rc}"

    return rc
}

def fetchStages(
    def tags, def overrides, def testResults, def rhcephversion=null,
    def metadataFilePath=null, def upstreamVersion=null
    ) {
    /*
        Return all the scripts found under
        cephci/pipeline/metadata/<MAJOR>.<MINOR>.yaml or
        cephci/pipeline/metadata/<upstreamVersion>.yaml matching
        the given tags as pipeline Test Stages.
           example: cephci/pipeline/metadata/5.1.yaml
        MAJOR   -   RHceph major version (ex., 5)
        MINOR   -   RHceph minor version (ex., 0)
        upstreamVersion - ex: pacific | quincy
    */
    println("Inside fetch stages from runner")
    def rhcephVersion
    if ( ! upstreamVersion ) {
        def RHCSVersion = [:]
        if ( overrides.containsKey("build") && overrides["build"] == "released" ) {
            RHCSVersion = fetchMajorMinorOSVersion("released")
        } else if ( rhcephversion ) {
            RHCSVersion["major_version"] = rhcephversion.substring(7,8)
            RHCSVersion["minor_version"] = rhcephversion.substring(9,10)
        } else {
            RHCSVersion = getRHCSVersionFromArtifactsNvr()
        }

        def majorVersion = RHCSVersion.major_version
        def minorVersion = RHCSVersion.minor_version
        rhcephVersion = "${majorVersion}.${minorVersion}"
    }
    else { rhcephVersion = upstreamVersion }

    def overridesStr = writeJSON returnText: true, json: overrides

    def runnerCLI = "cd ${env.WORKSPACE}/pipeline/scripts/ci;"
    runnerCLI = "${runnerCLI} ${env.WORKSPACE}/.venv/bin/python getPipelineStages.py"
    runnerCLI = "${runnerCLI} --rhcephVersion ${rhcephVersion}"
    runnerCLI = "${runnerCLI} --tags ${tags}"
    runnerCLI = "${runnerCLI} --overrides '${overridesStr}'"

    if(metadataFilePath){
        runnerCLI = "${runnerCLI} --metadata ${metadataFilePath}"
    }

    println("RunnerCLI: ${runnerCLI}")

    def testScriptString = sh (returnStdout: true, script: runnerCLI)

    println("testScriptString: ${testScriptString}")

    def testScripts = readYaml text: testScriptString
    def testStages = [:]

    if (! testScripts ){
        return testStages
    }

    testScripts["scripts"].each{scriptName, scriptData->
        testResults[scriptName] = [:]
        testStages[scriptName] = {
            stage(scriptName){
                testResults[scriptName]["status"] = executeTestScript(scriptData)
                testResults[scriptName]["logdir"] = scriptData["log_dir"]
            }
        }
    }
    def final_stage = testScripts["final_stage"]
    println("Final Stage after : ${final_stage}")
    println "Test Stages - ${testStages}"
    return ["testStages": testStages, "final_stage": final_stage]
}

def SendUMBMessage(def msgMap, def overrideTopic, def msgType) {
    /* Trigger a UMB message. */
    def msgContent = writeJSON returnText: true, json: msgMap
    def msgProperties = """ PRODUCT = Red Hat Ceph Storage
        TOOL = cephci
    """

    sendCIMessage ([
        providerName: 'Red Hat UMB',
        overrides: [topic: "${overrideTopic}"],
        messageContent: "${msgContent}",
        messageProperties: msgProperties,
        messageType: msgType,
        failOnError: true
    ])

}

return this;
