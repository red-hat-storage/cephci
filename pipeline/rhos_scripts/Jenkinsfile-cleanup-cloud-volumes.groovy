/*
    Pipeline script for clean up of orphaned volumes in rhos-d environment.
*/
// Global variables section

def nodeName = "centos-7"
def sharedLib

node(nodeName){
    stage('Install prereq') {
        checkout([
            $class: 'GitSCM',
            branches: [[name: 'origin/master']],
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
    stage('Scrub RHOS-Ring') {
        echo "Cleanup Orphaned cloud volumes  and Send Email to respective user"
        cmd = "${env.WORKSPACE}/.venv/bin/python ${env.WORKSPACE}/utility/cleanup_cloud_volumes.py --osp-cred ${env.HOME}/osp-cred-ci-2.yaml"
        echo cmd
        rc = sh(script: "${cmd}", returnStatus: true)
        if (rc != 0)
        {
            sh "echo \"stage failed with exit code : ${rc}\""
            currentBuild.result = 'FAILURE'
        }
    }
}
