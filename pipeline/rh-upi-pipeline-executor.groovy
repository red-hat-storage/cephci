/*
    Automated workflow for executing E2E test scenarios in UPI Pipeline.
*/
// Global variables section
def sharedLib
def versions = ""
def cephVersion = ""
def composeUrl = ""
def platform = ""
def clusterConf = ""
def rhBuild = ""
def testResults = [:]
def scenarioName = ""
def scenario = "1"

def getNodeList(def clusterConf, def sharedLib){
    def conf = sharedLib.yamlToMap(clusterConf, env.WORKSPACE)
    def nodesMap = conf.globals[0]["ceph-cluster"].nodes
    def nodeList = []

    nodesMap.eachWithIndex { item, index ->
        nodeList.add(item.hostname)
    }

    return nodeList
}

def executeTestSuite(def cmd) {
    def rc = "PASS"

    catchError(
        message: 'STAGE_FAILED',
        buildResult: 'FAILURE',
        stageResult: 'FAILURE'
    ) {
        try {
            sh (script: "${env.WORKSPACE}/${cmd}")
        } catch(Exception err) {
            rc = "FAIL"
            println err.getMessage()
            error "Encountered an error"
        }
    }
    println "exit status: ${rc}"

    return rc
}


def fetchSuites(
    def suites,
    def lodDir,
    def testResults,
    def stageName,
    def clusterConf,
    def rhBuild,
    def platform,
    def sharedLib
) {
    def testStagesSuites = [:]
    testResults[stageName] = [:]
    suites.each { item ->
        def suiteName = item.split("/").last().replace(".yaml", "")
        def suiteLogName = suiteName.replace('-','_').replace(' ', '_')
        def suiteLogDir = logDir + suiteLogName

        def cliArgs = ".venv/bin/python run.py"
        cliArgs += " --build tier-0"
        cliArgs += " --platform ${platform}"
        cliArgs += " --rhbuild ${rhBuild}"
        cliArgs += " --suite ${item}"
        cliArgs += " --cluster-conf ${clusterConf}"
        cliArgs += " --log-dir ${suiteLogDir}"
        cliArgs += " --log-level debug"
        cliArgs += " --skip-sos-report"
        cliArgs += " --cloud baremetal"
        println(cliArgs)

        testStagesSuites[suiteName] =  {
            stage(suiteName) {
                testResults[stageName][suiteName] = [
                    "logs": suiteLogDir ,
                    "result": executeTestSuite(cliArgs)
                ]
            }
        }
    }

    return testStagesSuites
}


def fetchStages(def scenarioId, def sharedLib) {
    def rhCephVersion = params.rhcephVersion //RHCEPH-6.0
    def testStages = [:]
    def majorMinorVersion = rhCephVersion.split('-').last()
    def dataContent = sharedLib.yamlToMap(
        "${majorMinorVersion}.yaml", "${env.WORKSPACE}/pipeline/baremetal"
    )

    dataContent.eachWithIndex { item, index ->
        println(item)
        println(item.scenario)
        println(item.stages)
        if ( "${item.scenario}" == "${scenarioId}" ) {
            println(item.stages)
            testStages =  item.stages
        }
    }
    return testStages
}

def reimageNodes(def platform, def nodelist) {
    // Reimage nodes in Octo lab
    sh (
        script: "bash ${env.WORKSPACE}/pipeline/scripts/ci/reimage-octo-node.sh --platform ${platform} --nodes ${nodelist}"
    )
}

def publishResults(scenario,testResults, rhBuild, scenarioName){
        println("Publish results for ${scenario}")
        sharedEmailLib = load(
            "${env.WORKSPACE}/pipeline/vars/bm_send_test_reports.groovy"
        )
        println(testResults)

        yamlData = readYaml(
            file: "/ceph/cephci-jenkins/latest-rhceph-container-info/${params.rhcephVersion}.yaml"
        )
        println(yamlData)
        println(sharedEmailLib)
        sharedEmailLib.sendEMail(testResults, rhBuild, scenarioName, yamlData["tier-0"])
}

def publishResultsStage(scenario,testResults, rhBuild, scenarioName){
    return stage("publishResults") {
                publishResults(scenario,testResults, rhBuild, scenarioName)
            }
}

def postBuildAction(sharedLib,scenario){
        def nextScenario = scenario.toInteger()+1
        def testStages = fetchStages(nextScenario.toString(), sharedLib)

        if (! testStages) {
            println("Completed all stages")
            return
        }

        build([
            wait: false,
            job: "rh-upi-pipeline-executor",
            parameters: [
                string(name: 'rhcephVersion', value: params.rhcephVersion),
                string(name: 'scenarioId', value: nextScenario.toString())
            ]
        ])
}

def postBuildActionStage(sharedLib,scenario){
    return stage("PostBuildAction") {
             postBuildAction(sharedLib,scenario)
             }
}

// Pipeline script entry point
node("magna006") {
    stage('prepareNode') {
        if (env.WORKSPACE) { sh script: "sudo rm -rf * .venv" }
        checkout(
            scm: [
                $class: 'GitSCM',
                branches: [[name: 'origin/upi_test_bare']],
                extensions: [[
                    $class: 'CleanBeforeCheckout',
                    deleteUntrackedNestedRepositories: true
                ], [
                    $class: 'WipeWorkspace'
                ], [
                    $class: 'CloneOption',
                    depth: 1,
                    noTags: true,
                    shallow: true,
                    timeout: 10,
                    reference: ''
                ]],
                userRemoteConfigs: [[
                    url: 'https://github.com/red-hat-storage/cephci.git'
                ]]
           ]
       )

        sharedLib = load("${env.WORKSPACE}/pipeline/vars/v3.groovy")
        sharedLib.prepareNode(0)
    }

    stage("reimageTestEnv") {
        def rhCephVersion = params.rhcephVersion    // RHCEPH-6.0
        scenario = params.scenarioId

        def majorMinorVersion = rhCephVersion.split('-').last()
        def confFileExists = sh (returnStatus: true, script: "ls -l ${env.WORKSPACE}/pipeline/baremetal/${majorMinorVersion}.yaml")
        if (confFileExists != 0) {
            error("${majorMinorVersion}.yaml does not exist.")
        }
        def dataContent = sharedLib.yamlToMap(
            "${majorMinorVersion}.yaml", "${env.WORKSPACE}/pipeline/baremetal"
        )

        dataContent.eachWithIndex { item, index ->
            if ( "${item.scenario}" == "${scenario}" ) {
                println(item.cluster_conf)
                clusterConf = item.cluster_conf
                rhBuild = item.rhbuild
                platform = item.platform
                scenarioName = item.name
                nodeList = getNodeList(item.cluster_conf, sharedLib)
                println(nodeList)
                def nodesWithSpace = nodeList.join(",")
                reimageNodes(item.os_version, nodesWithSpace)
            }
        }
    }


    testStages_1 = fetchStages(scenario, sharedLib)
    println(testStages_1)
    testStages_1.each { k, v ->
        logDir = "/ceph/cephci-jenkins/upi/${env.BUILD_NUMBER}/${k}/"
        def testJobStages = fetchSuites(
            v.suites, logDir, testResults, k, clusterConf, rhBuild, platform, sharedLib
        )

        abort_on_failure = v.abort_on_failure

        parallel testJobStages

        def stageResult = ""
        testResults[k].entrySet().each{
            if("${abort_on_failure}" == "true" && "${it.value["result"]}" == "FAIL"){
                println("ABORT all Stages")
//                 publishResultsStage(scenario,testResults, rhBuild, scenarioName)
                postBuildActionStage(sharedLib,scenario)
                error("${testResults[k]} failed so aborting all other stages")
            }
        }
        println(testResults)        
    }
//     publishResultsStage(scenario,testResults, rhBuild, scenarioName)
    postBuildActionStage(sharedLib,scenario)
}
