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
            sh(script: "${env.WORKSPACE}/${executeCLI}")
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
    def metadataFilePath=null
    ) {
    /*
        Return all the scripts found under
        cephci/pipeline/metadata/<MAJOR>.<MINOR>.yaml matching
        the given tierLevel as pipeline Test Stages.
           example: cephci/pipeline/metadata/5.1.yaml
        MAJOR   -   RHceph major version (ex., 5)
        MINOR   -   RHceph minor version (ex., 0)
    */
    println("Inside fetch stages from runner")
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
    def rhcephVersion = "${majorVersion}.${minorVersion}"

    def overridesStr = writeJSON returnText: true, json: overrides

    def runnerCLI = "cd ${env.WORKSPACE}/pipeline/scripts/ci;"
    runnerCLI = "${runnerCLI} ${env.WORKSPACE}/.venv/bin/python utility/getPipelineStages.py"
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

    testScripts.each{scriptName, scriptData->
        testResults[scriptName] = [:]
        testStages[scriptName] = {
            stage(scriptName){
                testResults[scriptName]["status"] = executeTestScript(scriptData)
            }
        }
    }

    println "Test Stages - ${testStages}"
    return testStages
}

return this;
