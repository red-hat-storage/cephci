#! /bin/env groovy
/*
    This script performs the required tasks for testing a new Ceph container image.

    This set of tasks are carried out when the underlying base image used by Red Hat
    Ceph Storage components have been modified.

    At a high level, the following tasks are performed
        - Jenkins agent node is prepared
        - The CI message is read to determine the build smoke test to be executed.
        - A release version of Red Hat Ceph Storage

    Reference:
      https://docs.engineering.redhat.com/display/CVP/Container+Verification+Pipeline+E2E+Documentation

    Metadata:
        env:            psi-only
        test:           Build Smoke test suite (tier-0)
        namespace:      rhceph-cvp-test
        type:           default
        category:       external1
        status:         PASSED - if all tests have passed else FAILED


*/
def sharedLib

def buildArgsMap = [:]
def testStatus

def versionNameMap = [ "4": "nautilus", "5": "pacific", "6": "quincy"]

def getProductVersion(Map arg) {
    /*
        Processes the build parameter to determine the product version.
        Returns:
            Map
    */
    def buildTag = arg.artifact.brew_build_tag
    def nvr = buildTag.split('-')       // ceph-5.1-rhel-8-candidate

    return [
        "product": nvr[0],
        "productVersion": nvr[1],
        "platform": "${nvr[2]}-${nvr[3]}",
        "type": nvr[4]
    ]
}

def postUMB(Map arg, String status) {
    // Posts a UMB event with the required body generated using the given information.

    def msgMap = [
        "category": "external1",
        "status": status,
        "ci": [
            "url": "${env.JENKINS_URL}",
            "team": "RH Ceph QE",
            "email": "cephci@redhat.com",
            "name": "RH CEPH"
        ],
        "run": [
            "url": "${env.JENKINS_URL}/job/${env.JOB_NAME}/${env.BUILD_NUMBER}/",
            "log": "${env.JENKINS_URL}/job/${env.JOB_NAME}/${env.BUILD_NUMBER}/console"
        ],
        "system": [
            "provider": "openstack",
            "os": "rhel"
        ],
        "artifact": [
            "nvr": arg.artifact.nvr,
            "scratch": "false",
            "component": arg.artifact.component,
            "type": "brew-build",
            "id": arg.artifact.id,
            "issuer": "rhceph-qe-ci"
        ],
        "test": [
            "type": "tier-0",
            "category": "validation",
            "result": status,
            "namespace": "rhceph.cvp.tier0.stage"
        ],
        "generated_at": "${env.BUILD_ID}",
        "pipeline": [
            "build": "${env.BUILD_NUMBER}",
            "name": arg.pipeline.name,
            "status": status
        ],
        "type": "default",
        "namespace": "rhceph-cvp-test",
        "version": "0.9.1"
    ]

    def msgContent = writeJSON returnText: true, json: msgMap
    sendCIMessage ([
        providerName: 'Red Hat UMB',
        overrides: [topic: 'VirtualTopic.eng.ci.brew-build.test.complete'],
        messageContent: "${msgContent}",
        messageProperties: "type=application/json",
        messageType: "Custom",
        failOnError: true
    ])
}

// Pipeline script entry point

node("rhel-8-medium") {

    stage('prepareNode') {
        if (env.WORKSPACE) {
            sh script: "sudo rm -rf * .venv"
        }
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

    stage("executeTest") {
        buildArgsMap = sharedLib.stringToMap(params.CI_MESSAGE)
        def productInfo = getProductVersion(buildArgsMap)
        def releaseName = versionNameMap[productInfo.productVersion.tokenize('.')[0]]

        def cmd = ".venv/bin/python"
        cmd = cmd.concat(" run.py")
        cmd = cmd.concat(" --osp-cred ${env.HOME}/osp-cred-ci-2.yaml --log-level debug")
        cmd = cmd.concat(" --rhbuild ${productInfo.productVersion}")
        cmd = cmd.concat(" --build rc --platform ${productInfo.platform}")
        cmd = cmd.concat(" --inventory conf/inventory/${productInfo.platform}-latest.yaml")
        cmd = cmd.concat(" --suite suites/${releaseName}/integrations/cvp.yaml")
        cmd = cmd.concat(" --global-conf conf/minimal.yaml")
        cmd = cmd.concat(" --docker-tag ${buildArgsMap.artifact.image_tag}")

        testStatus = sharedLib.executeTestSuite(cmd)
    }

    stage('postResults') {
        def status = "PASSED"
        if (testStatus.result == "FAIL") {
            status = "FAILED"
        }

        postUMB(buildArgsMap, status)

        def msg = "Container Verification Pipeline executed for "
        msg = msg.concat(
            "${buildArgsMap.artifact.brew_build_tag} completed with ${status}."
        )
        msg = msg.concat(
            " Log link ${env.JENKINS_URL}/job/${env.JOB_NAME}/${env.BUILD_NUMBER}/"
        )

        sharedLib.postGoogleChatNotification(msg)
    }
}
