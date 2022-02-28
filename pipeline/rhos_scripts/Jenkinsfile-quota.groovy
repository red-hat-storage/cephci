/*
    Pipeline script for fetching rhos-d quota.
*/
// Global variables section

def nodeName = "centos-7"
def sharedLib

node(nodeName){
    try {
        stage('Install prereq') {
            checkout([
                $class: 'GitSCM',
                branches: [[name: '*/master']],
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
                userRemoteConfigs: [[url: 'https://github.com/red-hat-storage/cephci.git']]
            ])
            sharedLib = load("${env.WORKSPACE}/pipeline/vars/common.groovy")
            sharedLib.prepareNode()
        }
        stage('Fetch quota and Send Email') {
            echo "Fetch quota and Send Email"
            cmd = "source ${env.WORKSPACE}/.venv/bin/activate;${env.WORKSPACE}/.venv/bin/python ${env.WORKSPACE}/rhos_quota.py --osp-cred ${env.HOME}/osp-cred-ci-2.yaml"
            echo cmd
            rc = sh(script: "${cmd}", returnStatus: true)
            if (rc != 0)
            {
                sh "echo \"stage failed with exit code : ${rc}\""
                currentBuild.result = 'FAILURE'
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
