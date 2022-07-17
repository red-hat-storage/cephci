// The primary objective of this script is to deploy a RHCeph cluster for OCS CI.

def rhcsVersionMap = [ "4": "nautilus", "5": "pacific", "6": "quincy" ]
def ciMap = [:]
def sharedLib
def vmPrefix

node ("rhel-8-medium || ceph-qe-ci") {

    stage("prepareJenkinsAgent") {
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
            ],
            changelog: false,
            poll: false
        )

        sharedLib = load("${env.WORKSPACE}/pipeline/vars/v3.groovy")
        sharedLib.prepareNode()
    }

    stage("deployCephCluster") {
        def cliArgs = ""
        ciMap = sharedLib.getCIMessageMap()

        majorVersion = ciMap.build.substring(0,1)
        upstreamName = rhcsVersionMap[majorVersion]
        clusterName = ciMap["cluster_name"]

        // Prepare the CLI arguments
        cliArgs += "--rhbuild ${ciMap.build}"
        cliArgs += " --platform rhel-8"
        cliArgs += " --build tier-0"
        cliArgs += " --skip-sos-report"
        cliArgs += " --inventory conf/inventory/rhel-8.5-server-x86_64-large.yaml"
        cliArgs += " --global-conf conf/${upstreamName}/integrations/7_node_ceph.yaml"
        cliArgs += " --suite suites/${upstreamName}/integrations/ocs.yaml"

        println "Debug: ${cliArgs}"

        returnStatus = sharedLib.executeTestSuite(cliArgs, false, true, clusterName)
        if ( returnStatus.result == "FAIL") {
            error "Deployment failed."
        }

        vmPrefix = returnStatus["instances-name"]
    }

    stage('postMessage') {
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
