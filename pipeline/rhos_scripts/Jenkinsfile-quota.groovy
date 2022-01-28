/*
    Pipeline script for fetching rhos-d quota.
*/
// Global variables section

def nodeName = "centos-7"
def sharedLib

node(nodeName){
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
}
