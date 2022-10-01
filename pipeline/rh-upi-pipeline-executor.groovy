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

def getNodeList(def clusterConf, def sharedLib) {
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

def fetchStages(
    def suites,
    def lodDir,
    def testResults,
    def stageName,
    def clusterConf,
    def rhBuild,
    def platform,
    def sharedLib
) {
    def testStages = [:]
    testResults[stageName] = [:]

    suites.each { item ->
        randomString = sharedLib.generateRandomString()
        suiteLogDir = logDir+randomString

        def suiteName = item.split("/").last().replace(".yaml", "")
        cliArgs = ".venv/bin/python run.py"
        cliArgs += " --build tier-0"
        cliArgs += " --platform ${platform}"
        cliArgs += " --rhbuild ${rhBuild}"
        cliArgs += " --suite ${item}"
        cliArgs += " --cluster-conf ${clusterConf}"
        cliArgs += " --log-dir ${suiteLogDir}"
        cliArgs += " --cloud baremetal"
        println(cliArgs)

        testStages[suiteName] =  {
            stage(suiteName) {
                testResults[stageName][suiteName] = [
                    "logs": suiteLogDir ,
                    "result": executeTestSuite(cliArgs)
                ]
            }
        }
    }

    return testStages
}

def fetchSuites(def scenarioId, def sharedLib) {
    def rhCephVersion = params.rhcephVersion //RHCEPH-6.0
    scenario = params.scenarioId

    def testStages = [:]
    def majorMinorVersion = rhCephVersion.split('-').last()
    def dataContent = sharedLib.yamlToMap(
        "${majorMinorVersion}.yaml", "${env.WORKSPACE}/pipeline/baremetal"
    )

    dataContent.eachWithIndex { item, index ->
        if ( "${item.scenario}" == "${scenario}" ) {
            def testSuites = item.suites

            testSuites.each { k,v ->
                def stageSuites = []
                v.eachWithIndex { i, id -> stageSuites.add(i) }
                testStages[k] = stageSuites
            }
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


// Pipeline script entry point
node("magna006") {

    stage('prepareNode') {
        if (env.WORKSPACE) { sh script: "sudo rm -rf * .venv" }
        checkout(
            scm: [
                $class: 'GitSCM',
                branches: [[name: 'origin/master']],
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


    def testSuites = fetchSuites(scenario, sharedLib)
    println(testSuites)
    testSuites.each { k, v ->
        logDir = "/ceph/cephci-jenkins/upi/${env.BUILD_NUMBER}/${k}/"
        def testJobStages = fetchStages(
            v, logDir, testResults, k, clusterConf, rhBuild, platform, sharedLib
        )

        parallel testJobStages
        println(testResults)
    }

    stage("publishResults") {
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

    stage("PostBuildAction") {
        def nextScenario = scenario.toInteger()+1
        def testStages = fetchSuites(nextScenario.toString(), sharedLib)

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
}
