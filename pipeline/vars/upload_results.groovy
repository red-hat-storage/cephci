import groovy.json.JsonOutput

/* Library module that contains methods to upload results to magna and report portal. */

def configureRpPreProc(
    def sharedLib,
    def rpPreprocFile=".rp_preproc_conf.yaml",
    def location="/ceph/cephci-jenkins"
    ) {
    /*
        This definition is to configure rclone to access IBM-COS
    */
    try {
        def tmpString = sharedLib.generateRandomString()
        def rp_preproc_dir = "${env.WORKSPACE}/rp_preproc-${tmpString}"

        sh script: "mkdir -p ${rp_preproc_dir}/payload"
        credsRpProc = sharedLib.yamlToMap(rpPreprocFile, location)

        return [rp_preproc_dir, credsRpProc]
    } catch(Exception err) {
        println err.getMessage()
        error "Encountered an error"
    }
}

def uploadTestResults(def sourceDir, def credPreproc, def runProperties, def stageLevel=null, def runType=null) {
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
    println("Inside uploadTestResults")
    def credFile = "${sourceDir}/config.json"

    // Configure rp_preproc launch
    def prefix = (runType == "upstream")? "Upstream " : ""
    def rhcs = (runType == "upstream")? runProperties["version"] : runProperties["version"].split('-')[1]
    def launchConfig = [
        "name": "${prefix}${runProperties['version']}",
        "description": "Test executed on ${runProperties['date']}",
        "attributes": [
            "ceph_version": runProperties["ceph_version"],
            "rhceph": rhcs,
            "build_type": runType
        ]
    ]
    if ( stageLevel && runType != "upstream") {
        launchConfig["name"] = runType + " " + launchConfig["name"] + " " + runProperties["ceph_version"]
    }
    credPreproc["reportportal"]["launch"] = launchConfig
    writeJSON file: credFile, json: credPreproc

    // Upload xml file to report portal
    println("calling rp_client")
    rp_launch_id = sh(returnStdout: true, script: ".venv/bin/python utility/rp_client.py -c ${credFile} -d ${sourceDir}/payload")
    println("rp_launch_id: ${rp_launch_id}")
    // Upload test result to polarion using xUnit Xml file
    withCredentials([
        usernamePassword(
            credentialsId: 'ceph-qe-sa',
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
    def launch_rgex = (rp_launch_id =~ /launch id: (\d+)/)
    println("launch_rgex : ${launch_rgex}")
	if(launch_rgex){
	    return launch_rgex[0][1]
	}
}

def getRPLaunches(def sourceDir, def credPreproc, def runProperties, def runType=null) {
    /*
        get launches from report portal based on query parameters

        Args:
            sourceDir       Working directory containing payload
            credPreproc     rp_preproc creds
            runProperties   Metadata information about the launch.
    */
    println("Inside getRPLaunches")
    def credFile = "${sourceDir}/config.json"
    def attributesFile = "${sourceDir}/attributes.json"

    def rhcs = (runType == "upstream")? runProperties["version"] : runProperties["version"].split('-')[1]
    writeJSON file: credFile, json: credPreproc

    attributes = [
        "ceph_version": runProperties["ceph_version"],
        "rhceph": rhcs,
        "build_type": runType
    ]
    def json = JsonOutput.toJson(attributes)
    json = JsonOutput.prettyPrint(json)
    writeJSON file: attributesFile, json: json
    rp_launches = sh(returnStdout: true, script: ".venv/bin/python utility/rp_client.py -c ${credFile} --attributes ${attributesFile}")
    def output = readJSON file: attributesFile
    return output['launches']
}

def mergeRPLaunches(def sourceDir, def credPreproc, def runProperties, def launchFile, def runType=null) {
    /*
        merge report portal launches
        Args:
            sourceDir       Working directory containing payload
            credPreproc     rp_preproc creds
            runProperties   Metadata information about the launch.
    */
    println("Inside mergeRPLaunches")
    def credFile = "${sourceDir}/config.json"
    println("credPreproc in mergeRPLaunches : ${credPreproc}")
    writeJSON file: credFile, json: credPreproc
    rp_launches = sh(returnStdout: true, script: ".venv/bin/python utility/rp_client.py -c ${credFile} --merge ${launchFile}")
    def output = readJSON file: launchFile
    return output['mergedLaunch']
}

def fetchTestItemIdForLaunch(
    def launchId, def rp_base_link, def sourceDir, def credPreproc, def runProperties, def stageLevel=null, def runType=null) {
    /*
        get Report Portal launcher details for the given launch id

        Args:
            launchId        The launch id for which details need to be fetched
            sourceDir       Working directory containing payload
            credPreproc     rp_preproc creds
            runProperties   Metadata information about the launch.
    */
    println("Inside fetchTestItemIdForLaunch")
    def credFile = "${sourceDir}/config.json"
    def outFile = "${sourceDir}/output.json"

    // Configure rp_preproc launch
    def prefix = (runType == "upstream")? "Upstream " : ""
    def rhcs = (runType == "upstream")? runProperties["version"] : runProperties["version"].split('-')[1]
    def launchConfig = [
        "name": "${prefix}${runProperties['version']}",
        "description": "Test executed on ${runProperties['date']}",
        "attributes": [
            "ceph_version": runProperties["ceph_version"],
            "rhceph": rhcs,
            "build_type": runType
        ]
    ]
    if ( stageLevel && runType != "upstream") {
        launchConfig["name"] = runType + " " + launchConfig["name"] + " " + runProperties["ceph_version"]
    }
    credPreproc["reportportal"]["launch"] = launchConfig

    println("Creds used")
    println(credPreproc)
    writeJSON file: credFile, json: credPreproc

    // Fetch rp launch details for launch id
    def cmd = "export RP_HOST_URL=${rp_base_link};"
    cmd = "${cmd} .venv/bin/python utility/rp_client.py -c ${credFile} -l ${launchId} -o ${outFile}"
    sh(returnStdout: true, script: cmd)
    rp_launch_details = readJSON file: "${outFile}"
    println("rp_launch_details: ${rp_launch_details}")
    return rp_launch_details
}

def writeToResultsFile(
    def cephVersion,
    def run_type,
    def tier,
    def stage,
    def testResults,
    def jenkinsBuildUrl,
    def reportPortalUrl,
    def rp_launch_details,
    def location="/ceph/cephci-jenkins/results"
) {
    /*
        Method to write results of execution to ceph version file.
    */
    println("jenkinsBuildUrl : ${jenkinsBuildUrl}")
    def type = run_type.replaceAll(" ", "_")

    def stageResults = [
        "build_url": "${jenkinsBuildUrl}",
        "test_results": rp_launch_details
    ]
    println("stageResults: ${stageResults}")
    def updatedResults = ["${stage}": stageResults]
    updatedResults = ["${tier}": updatedResults]
    updatedResults = ["${type}": updatedResults]
    println("updatedResults : ${updatedResults}")

    def resultsJson = writeJSON returnText: true, json: updatedResults

    println("resultsJson : ${resultsJson}")

    try {
        def cmd = "cd ${env.WORKSPACE}/pipeline/scripts/ci;"
        cmd = "${cmd} sudo ${env.WORKSPACE}/.venv/bin/python update_results.py"
        cmd = "${cmd} --cephVersion ${cephVersion}"
        cmd = "${cmd} --testResults '${resultsJson}'"

        sh (returnStdout: false, script: cmd)
    } catch(Exception exc) {
        println "Encountered a failure during updating results to results file."
        println exc
    }
}

def updateResultsFile(def sourceDir, def ceph_version, def run_type, def rp_link) {
    def type = run_type.replaceAll(" ", "_")
    println("type in groovy -- ${type}")

    linkAttributes = [
        "rp_link": rp_link,
        "build_type": type
    ]
    def resultsJson1 = writeJSON returnText: true, json: linkAttributes

    try {
        def cmd = "cd ${env.WORKSPACE}/pipeline/scripts/ci;"
        cmd = "${cmd} sudo ${env.WORKSPACE}/.venv/bin/python update_results.py"
        cmd = "${cmd} --cephVersion ${ceph_version} --rpLink '${resultsJson1}'"
        sh (returnStdout: false, script: cmd)
    } catch(Exception err) {
        println err.getMessage()
        error "Encountered an error during writing of results."
    }
}

def updateConfluencePage(
    def sharedLib,
    def majorVersion,
    def minorVersion,
    def cephVersion,
    def run_type,
    def testResults
) {
    /*
        Method to update test results to confluence page
    */
    println("Updating confluence page")
    rhcsVersion = "RHCS ${majorVersion}.${minorVersion}"
    pageContent = ["RHCS Version": rhcsVersion, "Ceph Version": cephVersion]
    type = run_type.replaceAll(" ", "_")

    textBody = testResults["${type}"].sort()
    textBody.each{k,v->
        println("key: ${k}")
        println("value: ${v}")
        if (k.indexOf("tier") >= 0) {
            def key = "${k}-${type}"
            def value = "PASS"
            def stageResults = v.sort()
            stageResults.each{ stage,result ->
                if (result["result"] == "FAILURE") {
                    // If any of the stages in a tier failed, then the status of the
                    // tier will be updated as failed
                    value = "FAIL"
                } else if ( stage == "stage-1" && result["result"] == "ABORTED" ) {
                    // If stage1 of a tier was aborted, then the status of the tier will
                    // be updated as skipped
                    value = "SKIP"
                }
            }
            pageContent.put(key, value)
        }
    }
    pageContentJson = writeJSON returnText: true, json: pageContent

    confMetadata = sharedLib.readFromConfluenceMetadata()
    title = confMetadata["pageTitle"]
    token = confMetadata["token"]
    space = confMetadata["space"]

    def cli = "cd ${env.WORKSPACE}/pipeline/scripts/ci;"
    cli = "${cli} ${env.WORKSPACE}/.venv/bin/python update_confluence.py"
    cli = "${cli} --content '${pageContentJson}'"
    cli = "${cli} --token '${token}'"
    cli = "${cli} --title '${title}'"
    cli = "${cli} --space '${space}'"

    println("Update Confluence CLI: ${cli}")

    def updateResult = sh (returnStdout: true, script: cli)
    println("Confluence page updated with content")
}

return this;
