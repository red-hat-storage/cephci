/*
    The primary objective of this script is to deploy a RHCS cluster to be used as a
    external application by integrating applications.
*/
def rhcsVersionMap = [ "4": "nautilus", "5": "pacific" ]
def ciMap = [:]

def baseInventoryPath = "conf/inventory"
def baseGlobalConfPath = "conf"
def baseSuitePath = "suites"
def sharedLib
def vmPrefix

// Defaults
def inventory = "rhel-8-latest.yaml"
def globalConf = "integrations/7_node_ceph.yaml"
def testSuite = "integrations/ocs/"

node ("centos-7") {

    timeout(unit: "MINUTES", time: 30) {
        stage("Prepare env") {
            if (env.WORKSPACE) {
                sh (script: "sudo rm -rf *")
            }

            checkout ([
                $class: 'GitSCM',
                branches: [[name: 'origin/master']],
                doGenerateSubmoduleConfigurations: false,
                extensions: [[
                    $class: 'CloneOption',
                    shallow: true,
                    noTags: true,
                    reference: '',
                    depth: 0
                ]],
                submoduleCfg: [],
                userRemoteConfigs: [[
                    url: 'https://github.com/red-hat-storage/cephci.git'
                ]]
            ])

            sharedLib = load("${env.WORKSPACE}/pipeline/vars/lib.groovy")
            sharedLib.prepareNode()
        }
    }

    stage("Deploy") {
        def cliArgs = "--v2"
        ciMap = sharedLib.getCIMessageMap()

        majorVersion = ciMap.build.substring(0,1)
        upstreamName = rhcsVersionMap[majorVersion]

        if (ciMap.containsKey("inventory")) {
            inventory = ciMap.inventory
        }

        if (ciMap.containsKey("testsuite")) {
            testSuite = ciMap.testsuite
        }

        if (ciMap.containsKey("global-conf")) {
            globalConf = ciMap["global-conf"]
        }

        // determine the platform
        def osName = sh (
            script: "cat ${baseInventoryPath}/${inventory} | grep -e '^id:' | cut -d ':' -f 2",
            returnStdout: true
        ).trim()
        def osMajorVersion = sh (
            script: "cat ${baseInventoryPath}/${inventory} | grep -e '^version_id:' | cut -d ':' -f 2 | cut -d '.' -f 1",
            returnStdout: true
        ).trim()
        def platform = "${osName}-${osMajorVersion}"

        // Prepare the CLI arguments
        cliArgs += " --rhbuild ${ciMap.build}"
        cliArgs += " --platform ${platform}"
        cliArgs += " --build tier-0"
        cliArgs += " --inventory ${baseInventoryPath}/${inventory}"
        cliArgs += " --global-conf ${baseGlobalConfPath}/${upstreamName}/${globalConf}"
        cliArgs += " --suite ${baseSuitePath}/${upstreamName}/${testSuite}"

        println "Debug: ${cliArgs}"

        returnStatus = sharedLib.executeTestSuite(cliArgs, false)
        if ( returnStatus.result == "FAIL") {
            error "Deployment failed."
        }

        vmPrefix = returnStatus["instances-name"]
    }

    stage('Publish') {
        def sutInfo = readYaml file: "sut.yaml"
        sutInfo["instances-name"] = vmPrefix

        def msgMap = [
            "artifact": [
                "type": "product-build",
                "name": "Red Hat Ceph Storage",
                "nvr": "RHCEPH-${ciMap.build}",
                "phase": "integration",
            ],
            "extra": sutInfo,
            "contact": [
                "name": "Downstream Ceph QE",
                "email": "cephci@redhat.com",
            ],
            "system": [
                "os": "centos-7",
                "label": "centos-7",
                "provider": "openstack",
            ],
            "pipeline": [
                "name": "rhceph-deploy-cluster",
                "id": currentBuild.number,
            ],
            "run": [
                "url": env.BUILD_URL,
                "log": "${env.BUILD_URL}console",
            ],
            "test": [
                "type": "integration",
                "category": "system",
                "result": currentBuild.currentResult,
            ],
            "generated_at": env.BUILD_ID,
            "version": "1.1.0"
        ]

        def msg = writeJSON returnText: true, json: msgMap
        println msg

        sharedLib.SendUMBMessage(
            msg,
            "VirtualTopic.qe.ci.rhcs.deploy.complete",
            "ProductBuildInStaging"
        )
    }
}
