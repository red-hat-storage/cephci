/*
    Pipeline script for executing IBM Tier x test suites for RH Ceph Storage.
*/

def nodeName = "agent-02"
def testStages = [:]
def testResults = [:]
def rhcephVersion
def buildArtifacts
def buildType
def buildPhase
def sharedLib


node(nodeName) {

    timeout(unit: "MINUTES", time: 30) {
        stage('prepareJenkinsAgent') {
            if (env.WORKSPACE) { sh script: "sudo rm -rf * .venv" }
            checkout(
                scm: [
                    $class: 'GitSCM',
                    branches: [[name: 'read_launch']],
                    extensions: [
                        [
                            $class: 'CleanBeforeCheckout',
                            deleteUntrackedNestedRepositories: true
                        ],
                        [
                            $class: 'WipeWorkspace'
                        ],
                        [
                            $class: 'CloneOption',
                            depth: 1,
                            noTags: true,
                            shallow: true,
                            timeout: 10,
                            reference: ''
                        ]
                    ],
                    userRemoteConfigs: [[
                        url: 'https://github.com/red-hat-storage/cephci.git'
                    ]]
                ],
                changelog: false,
                poll: false
            )
            // prepare the node
            sharedLib = load("${env.WORKSPACE}/pipeline/vars/lib.groovy")
            sharedLib.prepareIbmNode()
        }
    }

    stage('publishTestResult') {
         def tfacon =  sh(returnStdout: true, script:"~/.local/bin/tfacon")
         def tfacon1 =  sh(returnStdout: true, script:"whereis tfacon")
         def launch =  sh(returnStdout: true, script:".venv/bin/python utility/rp_client.py -c /tmp/config.json -d /tmp/payload")
         println "launch id : ${launch}"
         println "tfacon : ${tfacon}"
         println "tfacon : ${tfacon1}"

    }
}
