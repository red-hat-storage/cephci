/*
    Pipeline script for uploading IBM test run results to report portal.
*/

def nodeName = "centos-7"
def credsRpProc = [:]
def sharedLib
def rpPreprocDir
def reportBucket = "qe-ci-reports"
def remoteName= "ibm-cos"

node(nodeName) {

    timeout(unit: "MINUTES", time: 30) {
        stage('Install prereq') {
            if (env.WORKSPACE) {
                sh script: "sudo rm -rf *"
            }
            checkout([
                $class: 'GitSCM',
                branches: [[name: 'origin/master']],
                doGenerateSubmoduleConfigurations: false,
                extensions: [
                    [
                        $class: 'CloneOption',
                        shallow: true,
                        noTags: true,
                        reference: '',
                        depth: 1
                    ],
                    [$class: 'CleanBeforeCheckout'],
                ],
                submoduleCfg: [],
                userRemoteConfigs: [[
                    url: 'https://github.com/red-hat-storage/cephci.git'
                ]]
            ])

            // prepare the node
            sharedLib = load("${env.WORKSPACE}/pipeline/vars/lib.groovy")
            sharedLib.prepareNode()
        }
    }

    stage('Configure RP_PreProc'){
        (rpPreprocDir, credsRpProc) = sharedLib.configureRpPreProc()
    }

    stage('Upload xml to report-portal'){
        def configFile = sh(script: "rclone config file", returnStdout: true )
        def listFiles = sh(
            script: "rclone lsf ${remoteName}:${reportBucket} -R --files-only",
            returnStdout: true
        )
        if (listFiles){
            def fileNames = listFiles.split("\\n")
            for (fName in fileNames){
                println("Upload ${fName} to report-portal.......")
                sharedLib.uploadXunitXml(fName, credsRpProc, rpPreprocDir)
            }
        }
    }
}
