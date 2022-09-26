/*
    Identify the latest development build not older than a week and trigger the UPI
    pipeline for all releases that have a new build artifact.
*/
def recipeDir = "/ceph/cephci-jenkins/latest-rhceph-container-info/"
def recipeFile = "RHCEPH-*.yaml"

node("magna006") {

    stage('prepareJenkinsAgent') {

        timeout(unit: "MINUTES", time: 10) {
            sh '''
                if [ ! -d /ceph/cephci-jenkins ]; then
                    sudo mkdir -p /ceph
                    sudo mount -t nfs -o sec=sys,nfsvers=4.1 reesi004.ceph.redhat.com:/ /ceph
                fi
            '''
        }
    }

    stage('triggerBuild') {

        def cmdOut = sh (
            returnStdout: true,
            script: "find ${recipeDir} -name ${recipeFile} -mtime -7 -print"
        )

        if ( ! cmdOut ) {
            currentBuild.result = 'ABORTED'
            error("There are no new development builds this week.")
        }

        def recipeFiles = cmdOut.split('\n')
        for (rFile in recipeFiles) {
            def rhCephVersion = rFile.split('/').last().replace('.yaml', '')
            println("Triggering UPI Pipeline for ${rhCephVersion}")

            build([
                wait: false,
                job: "rhceph-upi-pipeline-executor",
                parameters: [
                    string(name: 'rhcephVersion', value: rhCephVersion),
                    string(name: 'scenarioId', value: '1')
                ]
            ])
        }

    }

}
