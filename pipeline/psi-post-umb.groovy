/*
    Post message to UMB for all RHCEPH releases that are passed FeatureComplete stage.

    A message is posted when a recipe file was modified in the last 7 days. The list of
    modified releases are then compared against a the list of feature complete releases.
    A message is posted for the filtered list.
*/
def featureCompleteReleases = [
    'RHCEPH-5.3.yaml',
    'RHCEPH-6.0.yaml',
    'RHCEPH-6.1.yaml',
    'RHCEPH-7.0.yaml'
]

node('rhel-9-medium || ceph-qe-ci') {
    stage('prepareNode') {
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
        timeout(unit: "MINUTES", time: 10) {
            sh '''
                if [ ! -d /ceph/cephci-jenkins ]; then
                    sudo mkdir -p /ceph
                    sudo mount -t nfs -o sec=sys,nfsvers=4.1 reesi004.ceph.redhat.com:/ /ceph
                fi
            '''
        }
    }

    stage('postMessage') {
        def umbLib = load("${env.WORKSPACE}/pipeline/vars/umb.groovy")
        def findRootDir = "/ceph/cephci-jenkins/latest-rhceph-container-info/"

        // Check if there was a new build in the last week for each supported release
        featureCompleteReleases.each {
            def out = sh(
                script: "find $findRootDir -name $it -mtime -7 -print",
                returnStdout: true
            )
            if (out?.trim()) {
                def recipeMap = readYaml file: out.trim()
                def nvr = it.replace(".yaml", "")

                println("Development build found for $nvr")

                umbLib.postUMBTestQueue(nvr, recipeMap, "true")
            }
        }
    }
}
