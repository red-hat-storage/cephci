/*
    Pipeline script for executing long running cephci test suites.
*/

def nodeName = "centos-7"
def testStages = [:]
def testResults = [:]
def rhcephVersion
def buildArtifacts
def buildType
def buildPhase
def sharedLib


node(nodeName) {
    try {
        timeout(unit: "MINUTES", time: 30) {
            stage('Install prereq') {
                if (env.WORKSPACE) { sh script: "sudo rm -rf * .venv" }
                checkout(
                    scm: [
                        $class: 'GitSCM',
                        branches: [[name: 'origin/master']],
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
                sharedLib.prepareNode()
            }
        }

        stage("PrepareTestStages") {
            /* Prepare pipeline stages using RHCEPH version */
            rhcephVersion = "${params.rhcephVersion}" ?: ""
            buildType = "${params.buildType}" ?: ""
            buildArtifacts = "${params.buildArtifacts}" ?: [:]

            if ( buildArtifacts ){
                buildArtifacts = readJSON text: "${buildArtifacts}"
            }
            println "buildArtifacts : ${buildArtifacts}"

            if ( (! rhcephVersion?.trim()) && (! buildType?.trim()) ) {
                error "Required Prameters are not provided.."
            }

            testStages = sharedLib.fetchStages(
                "--build latest --xunit-results",
                buildType,
                testResults,
                rhcephVersion,
                'pipeline/scripts/cron'
            )

            if ( testStages.isEmpty() ) {
                currentBuild.result = "ABORTED"
                error "No test stages found.."
            }
            currentBuild.description = "${params.rhcephVersion} - ${buildType}"
            println "currentBuild.description : ${currentBuild.description}"
        }

        parallel testStages

        stage('Publish Results') {
            /* Publish results through E-mail and Google Chat */

            def status = "STABLE"
            if ('FAIL' in sharedLib.fetchStageStatus(testResults)) {
                status = "UNSTABLE"
            }
            def msg= "Run for ${rhcephVersion} Cron Pipeline ${buildType.capitalize()} is ${status}.Log:${env.BUILD_URL}"
            googlechatnotification(url: "id:rhcephCIGChatRoom", message: msg)

            def releaseContent = [
                "composes": buildArtifacts["composes"],
                "product": "Red Hat Ceph Storage",
                "version": rhcephVersion,
                "ceph_version": buildArtifacts["ceph-version"],
                "repository": buildArtifacts["repository"]
            ]
            sharedLib.sendEmail(
                testResults,
                releaseContent,
                buildType.capitalize(),
                "Cron Pipeline"
            )
        }

        stage('Publish UMB') {
            /* send UMB message */

            def artifactsMap = [
                "artifact": [
                    "type": "product-build",
                    "name": "Red Hat Ceph Storage",
                    "version": buildArtifacts["ceph-version"],
                    "nvr": rhcephVersion,
                    "phase": "testing",
                    "build": buildType,
                ],
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
                    "name": "rhceph-cron-test-executor",
                    "id": currentBuild.number,
                ],
                "run": [
                    "url": env.BUILD_URL,
                    "log": "${env.BUILD_URL}console",
                    "additional_urls": [
                        "doc": "https://docs.engineering.redhat.com/display/rhcsqe/RHCS+QE+Pipeline",
                        "repo": "https://github.com/red-hat-storage/cephci",
                        "report": "https://reportportal-rhcephqe.apps.ocp4.prod.psi.redhat.com/",
                        "tcms": "https://polarion.engineering.redhat.com/polarion/",
                    ],
                ],
                "test": [
                    "type": buildType,
                    "category": "functional",
                    "result": currentBuild.currentResult,
                ],
                "generated_at": env.BUILD_ID,
                "version": "1.0.0"
            ]

            def msgType = "Tier2IntegrationTestingDone"

            def msgContent = writeJSON returnText: true, json: artifactsMap
            println "${msgContent}"

            sharedLib.SendUMBMessage(
                artifactsMap,
                "VirtualTopic.qe.ci.rhcephqe.product-build.test.complete",
                msgType,
            )
        }

        stage('postBuildAction') {
            def buildPhaseValue = buildType.split("-")
            buildPhase = buildPhaseValue[1].toInteger()+1
            buildPhase = buildPhaseValue[0]+"-"+buildPhase
            buildArtifacts = writeJSON returnText: true, json: buildArtifacts

            build ([
                wait: false,
                job: "rhceph-cron-pipeline-test-executor",
                parameters: [string(name: 'rhcephVersion', value: rhcephVersion),
                            string(name: 'buildType', value: buildPhase),
                            string(name: 'buildArtifacts', value: buildArtifacts)]
            ])

            if ("FAIL" in sharedLib.fetchStageStatus(testResults)) {
                currentBuild.result = "FAILED"
                error "Failure occurred in current run.."
            }

        }

    } catch(Exception err) {
        if (currentBuild.result != "ABORTED") {
            // notify about failure
            currentBuild.result = "FAILURE"
            def failureReason = err.getMessage()
            def subject =  "[CEPHCI-PIPELINE-ALERT] [JOB-FAILURE] - ${env.JOB_NAME}/${env.BUILD_NUMBER}"
            def body = "<body><h3><u>Job Failure</u></h3></p>"
            body += "<dl><dt>Jenkins Build:</dt><dd>${env.BUILD_URL}</dd>"
            body += "<dt>Failure Reason:</dt><dd>${failureReason}</dd></dl></body>"

            emailext (
                mimeType: 'text/html',
                subject: "${subject}",
                body: "${body}",
                from: "cephci@redhat.com",
                to: "ceph-qe@redhat.com"
            )
            subject += "\n Jenkins URL: ${env.BUILD_URL}"
            googlechatnotification(url: "id:rhcephCIGChatRoom", message: subject)
        }
    }
}
