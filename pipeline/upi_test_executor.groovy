// Workflow to execute tests
def sharedLib
def retVal
def run_type = "RHCEPH-upi-test-executor run"
def rhcephVersion= "${params.RHCS_Build}"
def phase = "${params.Build}"
def group = "${params.Group}-"
def suite = "${params.Suite}"
def os_version = "${params.os_version}"
def buildUserId
def buildUserEmail
def buildUserName

def getCLI(sharedLib,rhcephVersion){
    /*
        Generates the CLI using the arguments provided and returns it.
    */

    cephVersion = sharedLib.get_ceph_version(rhcephVersion)

    def cli = "--rhbuild ${params.RHCS_Build}"
    cli += " --platform ${params.Platform}"
    cli += " --build ${params.Build}"
    cli += " --global-conf conf/${cephVersion}/${params.Group}/${params.Conf}.yaml"
    cli += " --inventory conf/inventory/${params.Inventory}.yaml"
    cli += " --suite suites/${cephVersion}/${params.Group}/${params.Suite}.yaml"
    cli += " --cloud baremetal"
    cli += " ${params.Additional_Args}"

    if(params.Override_Ceph_Base_URL?.trim()){
        cli += " --rhs-ceph-repo ${params.Override_Ceph_Base_URL}"
    }
    if(params.Additional_Repo_File?.trim()){
        cli += " --add-repo ${params.Additional_Repo_File}"
    }
    if(params.Hotfix_Repo_File?.trim()){
        cli += " --hotfix-repo ${params.Hotfix_Repo_File}"
    }
    if(params.Override_Container_Image?.trim()){
        container = params.Override_Container_Image.split(".com/")
        docker_registry = container[0] + ".com"
        docker = container[1].split(":")
        docker_tag = docker.last()
        docker_image = docker.first()
        cli += " --insecure-registry"
        cli += " --docker-registry " + docker_registry
        cli += " --docker-tag " + docker_tag
        cli += " --docker-image " + docker_image
    }
    return cli
}

def buildArtifactDetails(def sharedLib){
    String[] majorMinorVersion = "${params.RHCS_Build}".tokenize(".")
    releaseInfo = sharedLib.readFromReleaseFile(majorMinorVersion[0], majorMinorVersion[1], false)
    def baseUrl = params.Override_Ceph_Base_URL
    def cephVersion = ""
    if(baseUrl?.trim()){
        cephVersion = sharedLib.fetchCephVersion(params.Override_Ceph_Base_URL)
    }
    else{
        baseUrl = releaseInfo[params.Build]["composes"][params.Platform]
        cephVersion = releaseInfo[params.Build]["ceph-version"]
    }
    def containerImage = params.Override_Container_Image
    if(!containerImage?.trim()){
        containerImage = releaseInfo[params.Build]["repository"]
    }

    artifactDetails = [
        "product": "Red Hat Ceph Storage",
        "version": "RHCEPH-${params.RHCS_Build}",
        "ceph_version": cephVersion,
        "container_image": containerImage,
        "composes": [
            "${params.Platform}": baseUrl
        ]
    ]
    return artifactDetails
}

node("magna006"){
    stage('Install pre req') {
        if (env.WORKSPACE) { sh script: "sudo rm -rf * .venv" }
        checkout([
            $class: 'GitSCM',
            branches: [[name: "*/${params.Branch}"]],
            doGenerateSubmoduleConfigurations: false,
            extensions: [[
                $class: 'SubmoduleOption',
                disableSubmodules: false,
                parentCredentials: false,
                recursiveSubmodules: true,
                reference: '',
                trackingSubmodules: false
            ]],
            submoduleCfg: [],
            userRemoteConfigs: [[
                url: "${params.Git_Repo}"
            ]]
        ])
        sharedLib = load("${env.WORKSPACE}/pipeline/vars/v3.groovy")
        if(params.Destroy_Cluster != 'Destroy always'){
            sharedLib.prepareNode(0,"ceph-ci")
        }
        else{
            sharedLib.prepareNode()
        }
        def buildUser = sharedLib.getBuildUser()
        buildUserId = buildUser["buildUserId"]
        buildUserEmail = buildUser["buildUserEmail"]
        buildUserName = buildUser["buildUserName"]
        currentBuild.description = "Triggered by : ${buildUserName}"
    }
    stage("reimageTestEnv") {
        cephVersion = sharedLib.get_ceph_version(rhcephVersion)
        nodeList = sharedLib.getNodeList("conf/${cephVersion}/${params.Group}/${params.Conf}.yaml")
        println(nodeList)
        def nodesWithSpace = nodeList.join(",")
        sharedLib.reimageNodes(os_version, nodesWithSpace)
    }

    stage('Execute groovy script'){
        script{
            def cleanupOnSuccess = false
            def cleanupOnFailure = false
            def cli = getCLI(sharedLib,rhcephVersion)
            retVal = sharedLib.executeTestSuite(cli, cleanupOnSuccess, cleanupOnFailure, buildUserId)
            retVal.put("status", retVal["result"])
            println(retVal)
        }
    }
    stage('Publish Results'){
        testResults = ["${params.Suite}" : retVal]
        def (majorVersion, minorVersion) = rhcephVersion.tokenize(".")
        def releaseContent = sharedLib.readFromReleaseFile(majorVersion, minorVersion, lockFlag=false)
        sharedLib.sendEmail(
                run_type,
                testResults,
                sharedLib.buildArtifactsDetails(releaseContent, rhcephVersion, phase),
                group,
                suite,
                buildUserEmail
        )
    }
}
