// The primary objective of this script is to deploy a RHCeph cluster for OCS CI.

def argsMap = [
    "4": [
        "inventory": "conf/inventory/rhel-8.7-server-x86_64-large.yaml",
        "globalConf": "conf/nautilus/integrations/7_node_ceph.yaml",
        "suite": "suites/nautilus/integrations/ocs.yaml",
        "platform": "rhel-8"
    ],
    "5": [
        "inventory": "conf/inventory/rhel-8.7-server-x86_64-large.yaml",
        "globalConf": "conf/pacific/integrations/7_node_ceph.yaml",
        "suite": "suites/pacific/integrations/ocs.yaml",
        "platform": "rhel-8",
        "rgwSecure": "suites/pacific/integrations/ocs_rgw_ssl.yaml",
    ],
    "6": [
        "inventory": "conf/inventory/rhel-9.4-server-x86_64-xlarge.yaml",
        "globalConf": "conf/quincy/integrations/7_node_ceph.yaml",
        "suite": "suites/quincy/integrations/ocs.yaml",
        "platform": "rhel-9",
        "rgwSecure": "suites/quincy/integrations/ocs_rgw_ssl.yaml",
        "overrides": [
            "grafana_image": "registry-proxy.engineering.redhat.com/rh-osbs/grafana:ceph-6.0-rhel-9-containers-candidate-99494-20221026123006",
            "promtail_image": "registry-proxy.engineering.redhat.com/rh-osbs/promtail:ceph-6.0-rhel-9-containers-candidate-10191-20221026120801",
            "haproxy_image": "registry-proxy.engineering.redhat.com/rh-osbs/haproxy:ceph-6.0-rhel-9-containers-candidate-53939-20221026121907",
            "keepalived_image": "registry-proxy.engineering.redhat.com/rh-osbs/keepalived:ceph-6.0-rhel-9-containers-candidate-18945-20221026120854",
            "snmp_gateway_image": "registry-proxy.engineering.redhat.com/rh-osbs/snmp-notifier:ceph-6.0-rhel-9-containers-candidate-15559-20221026120853"
        ]
    ],
    "7": [
        "inventory": "conf/inventory/rhel-9.4-server-x86_64-xlarge.yaml",
        "globalConf": "conf/reef/integrations/7_node_ceph.yaml",
        "suite": "suites/reef/integrations/ocs.yaml",
        "platform": "rhel-9",
        "rgwSecure": "suites/reef/integrations/ocs_rgw_ssl.yaml",
        "overrides": [
            "grafana_image": "registry-proxy.engineering.redhat.com/rh-osbs/grafana:10.4.0-15",
            "haproxy_image": "registry-proxy.engineering.redhat.com/rh-osbs/haproxy:2.4.22-27",
            "keepalived_image": "registry-proxy.engineering.redhat.com/rh-osbs/keepalived:2.2.8-26",
            "snmp_gateway_image": "registry-proxy.engineering.redhat.com/rh-osbs/snmp-notifier:1.2.1-76",
            "promtail_image": "registry-proxy.engineering.redhat.com/rh-osbs/promtail:v2.4.0-34"
        ]
    ]
]
def ciMap = [:]
def sharedLib
def vmPrefix

node ("rhel-9-medium || ceph-qe-ci") {

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

        majorVersion = ciMap.rhbuild.substring(0,1)
        clusterName = ciMap["cluster_name"]
        def buildType = "${ciMap.build}" ?: "tier-0"

        // Prepare the CLI arguments
        cliArgs += "--rhbuild ${ciMap.rhbuild}"
        cliArgs += " --platform ${argsMap[majorVersion]['platform']}"
        cliArgs += " --build ${buildType}"
        cliArgs += " --skip-sos-report"
        cliArgs += " --inventory ${argsMap[majorVersion]['inventory']}"
        cliArgs += " --global-conf ${argsMap[majorVersion]['globalConf']}"

        if ( ciMap.containsKey("rgw_secure") && ciMap["rgw_secure"] ) {
            cliArgs += " --suite ${argsMap[majorVersion]['rgwSecure']}"
        } else {
            cliArgs += " --suite ${argsMap[majorVersion]['suite']}"
        }

        if ( argsMap[majorVersion].containsKey("overrides") ) {
            def overrides = argsMap[majorVersion]['overrides']
            overrides.each { k, v -> cliArgs += " --custom-config ${k}=${v}" }
        }

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
                "nvr": "RHCEPH-${ciMap.rhbuild}",
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
