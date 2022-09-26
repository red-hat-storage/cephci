/*
    Script to execute the cephci on baremetals
*/
// Global variables section
def sharedLib
def versions
def cephVersion
def composeUrl
def platform
def clusterConf
def rhbuild
def testResults = [:]
def scenarioName

def getNodeList(clusterConf,sharedLib){
    def conf = sharedLib.yamlToMap(clusterConf,"${env.WORKSPACE}")
    def nodesMap = conf.globals[0]["ceph-cluster"].nodes
    def nodeList = []
    nodesMap.eachWithIndex { item, index ->
    nodeList.add(item.hostname)
    }
    return nodeList
}
def executeTestSuite(cmd_cli){
    def rc = "PASS"
    def executeCLI = cmd_cli
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
        }
    }
    println "exit status: ${rc}"
    return rc
}

def fetchStages(sharedLib,suites,log_dir,testResults,stageName,clusterConf,rhbuild,platform){
    stages = [:]
    testResults[stageName] = [:]
    suites.each{item ->
        randomString = sharedLib.generateRandomString()
        suitelog_dir = log_dir+randomString
        def suiteName = item.split("/").last().replace(".yaml","")
        cmd_cli = ".venv/bin/python run.py --osp-cred ${HOME}/osp-cred-ci-2.yaml --build tier-0 --platform ${platform} --rhbuild ${rhbuild} --suite ${item} --cluster-conf ${clusterConf} --log-dir ${suitelog_dir} --cloud baremetal"
        println(cmd_cli)
        stages[suiteName] =  {stage(suiteName){testResults[stageName][suiteName]=["Logs": suitelog_dir ,"Result":executeTestSuite(cmd_cli)]}}
    }
    return stages
    }
def fetchSuites(scenarioId,sharedLib){
    rhcephVersion = "${params.rhcephVersion}" //RHCEPH-6.0
    def stages = [:]
    def datacontent = sharedLib.yamlToMap("${rhcephVersion.split('-')[1]}.yaml","${env.WORKSPACE}/pipeline/baremetal/")
    datacontent.eachWithIndex { item, index ->
        if ("${item.scenario}" == "${scenarioId}"){
        def suites = item.suites
        suites.each{k,v ->
        def stage_suite = []
        v.eachWithIndex {i, id ->
        stage_suite.add(i)
        }
        stages[k] = stage_suite
        }
        }
    }
    return stages

}
// Pipeline script entry point
node("magna006") {
        stage('prepareNode') {sharedLib
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
                   ],)

            sharedLib = load("${env.WORKSPACE}/pipeline/vars/v3.groovy")
            sharedLib.prepareNode(0)
        }
        stage("ReimageNodes") {
        scenario = "${params.scenarioId}"
        rhcephVersion = "${params.rhcephVersion}" //RHCEPH-6.0
        def datacontent = sharedLib.yamlToMap("${rhcephVersion.split('-')[1]}.yaml","${env.WORKSPACE}/pipeline/baremetal/")
        datacontent.eachWithIndex { item, index ->
        if ("${item.scenario}" == "${scenario}"){
        println("${item.cluster_conf}")
        clusterConf = item.cluster_conf
        rhbuild = item.rhbuild
        platform = item.platform
        scenarioName = item.name
        nodeList = getNodeList(clusterConf,sharedLib)
        println("${nodeList}")
        sharedLib.reimageNodes(item.os_version,nodeList.join(","))
        }
        }
        }
        stage("fetchTestSuites") {
        stages = fetchSuites(scenario,sharedLib)
        println(stages)
        stages.each{k,v ->
        logdir = "/ceph/cephci-jenkins/upi/${env.BUILD_NUMBER}/${k}/"
        stages = fetchStages(sharedLib,v,logdir,testResults,k,clusterConf,rhbuild,platform)
        parallel stages
        println(testResults)
        }
        }
        stage("publish results")
        {
        println("Publish results for ${scenario}")
        sharedEmailLib = load("${env.WORKSPACE}/pipeline/vars/bm_send_test_reports.groovy")
        println(testResults)
        yamlData = readYaml file: "/ceph/cephci-jenkins/latest-rhceph-container-info/${params.rhcephVersion}.yaml"
        println(yamlData)
        println(sharedEmailLib)
        sharedEmailLib.sendEMail(testResults,rhbuild,scenarioName,yamlData["tier-0"])
        }
        stage("PostBuildAction"){
        def nextScenario = scenario.toInteger()+1
        stages = fetchSuites(nextScenario.toString(),sharedLib)
        if (stages){
         build([
                wait: false,
                job: "rh-upi-pipeline-executor.groovy",
                parameters: [
                    string(name: 'rhcephVersion', value: "${params.rhcephVersion}"),
                    string(name: 'scenarioId', value: nextScenario.toString())
                ]
            ])
            }
         else{
         println("Completed all stages")
         }
        }
    }
