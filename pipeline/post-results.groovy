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
        stage('prepareJenkinsAgent') {
            if (env.WORKSPACE) { sh script: "sudo rm -rf * .venv" }
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

    stage('configureReportPortalWorkDir') {
        (rpPreprocDir, credsRpProc) = sharedLib.configureRpPreProc()
    }

    stage('uploadTestResult') {
        def msgMap = sharedLib.getCIMessageMap()
        def composeInfo = msgMap["recipe"]

        def resultDir = msgMap["test"]["object-prefix"]
        println("Test results are available at ${resultDir}")

        def tmpDir = sh(returnStdout: true, script: "mktemp -d").trim()
        sh script: "rclone sync ${remoteName}://${reportBucket} ${tmpDir} --progress --create-empty-src-dirs"

        def metaData = readYaml file: "${tmpDir}/${resultDir}/metadata.yaml"
        def copyFiles = "cp -a ${tmpDir}/${resultDir}/results ${rpPreprocDir}/payload/"
        def rmTmpDir = "rm -rf ${tmpDir}"

        // Modifications to reuse methods
        metaData["ceph_version"] = metaData["ceph-version"]
        if ( metaData["stage"] == "latest" ) { metaData["stage"] = "Tier-0" }

        sh script: "${copyFiles} && ${rmTmpDir}"

        if ( composeInfo ){
            metaData["buildArtifacts"] = composeInfo
        }
        sharedLib.sendEmail(metaData["results"], metaData, metaData["stage"])
        sharedLib.uploadTestResults(rpPreprocDir, credsRpProc, metaData)

        //Remove the sync results folder
        sh script: "rclone purge ${remoteName}:${reportBucket}/${resultDir}"

        // Update RH recipe file
        def testStatus = msgMap["test"]["result"]

        if ( composeInfo != null && testStatus == "SUCCESS" ){
            def tierLevel = msgMap["pipeline"]["name"]
            def rhcsVersion = sharedLib.getRHCSVersionFromArtifactsNvr()
            majorVersion = rhcsVersion["major_version"]
            minorVersion = rhcsVersion["minor_version"]

            def latestContent = sharedLib.readFromReleaseFile(
                    majorVersion, minorVersion
                )

            if ( latestContent.containsKey(tierLevel) ) {
                latestContent[tierLevel] = composeInfo
            }
            else {
                def updateContent = ["${tierLevel}": composeInfo]
                latestContent += updateContent
            }
            sharedLib.writeToReleaseFile(majorVersion, minorVersion, latestContent)
        }
    }

}
