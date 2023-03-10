/*
    This workflow performs the following tasks

        - Process the Ceph - OSP integration test result message posted.
        - If the test run was successful update the recipe file
        - Send an email to cephci@redhat.com about the test results
*/
def sendEMail(def bodyMap) {
    /*
        This method posts an email using the provided arguments.

        Args:
          bodyMap (map):    Map that contains the following keys
                                artifact:  Information of build used for testing
                                test:   Details about the test
                                run:    Details about the run
    */
    def htmlMail = readFile("pipeline/vars/emailable-report.html")
    htmlMail += "<body>"
    htmlMail += "<h3><u>Summary</u></h3>"
    htmlMail += "<table>"
    htmlMail += "<tr><td>Product</td><td>${bodyMap['artifact']['nvr']}</td></tr>"
    htmlMail += "<tr><td>Ceph version</td><td>${bodyMap['artifact']['ceph-version']}</td></tr>"
    htmlMail += "<tr><td>Composes</td><td>${bodyMap['artifact']['composes']}</td></tr>"
    htmlMail += "<tr><td>Image</td><td>${bodyMap['artifact']['repository']}</td></tr>"
    htmlMail += "<tr><td>OSP version</td><td>${bodyMap['nvr']}</td></tr>"
    htmlMail += "<tr><td>OSP Integration status</td><td>${bodyMap['test']['result']}</td></tr>"
    htmlMail += "<tr><td>Logs</td><td>${bodyMap['run']['log']}</td></tr>"
    htmlMail += "</table> <br /> </body> </html>"

    emailext (
        mimeType: 'text/html',
        subject: "[Test Report] ${bodyMap['nvr']} - ${bodyMap['artifact']['nvr']} InterOp test results.",
        body: "${htmlMail}",
        from: "cephci@redhat.com",
        to: "cephci@redhat.com"
    )
}

node('rhel-9-medium || ceph-qe-ci') {

    stage('prepareJenkinsNode') {

        if ( !params.CI_MESSAGE?.trim() ) {
            currentBuild.result = 'ABORTED'
            error('No test execution results to process.')
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
    }

    stage('publish') {

        def argMap = readJSON text: params.CI_MESSAGE
        sendEMail(argMap)

        if ( argMap['test']['result'] == 'FAILED' ) {
            println('New build failed to pass tests.')
            return
        }

        timeout(unit: "MINUTES", time: 10) {
            sh '''
                if [ ! -d /ceph/cephci-jenkins ]; then
                    sudo mkdir -p /ceph
                    sudo mount -t nfs -o sec=sys,nfsvers=4.1 reesi004.ceph.redhat.com:/ /ceph
                fi
            '''
        }

        def ospMap = [
            "repository": argMap['artifact']['repository'],
            "composes": argMap['artifact']['composes'],
            "ceph-version": argMap['artifact']['ceph-version']
        ]

        def nvr = argMap['artifact']['nvr']
        def rhCephVersion = nvr.split('-')[1]
        def majorVersion = rhCephVersion.split('\\.')[0]
        def minorVersion = rhCephVersion.split('\\.')[1]

        def sharedLib = load("${env.WORKSPACE}/pipeline/vars/v3.groovy")
        def recipeMap = sharedLib.readFromReleaseFile(majorVersion, minorVersion)

        if ( recipeMap?.osp ) {
            def existingCephVersion = recipeMap['osp']['ceph-version']
            def newCephVersion = ospMap['ceph-version']
            def rst = sharedLib.compareCephVersion(existingCephVersion, newCephVersion)

            if ( rst != 1 ) {
                println("Tests were not executed on a newer development version.")
                sharedLib.unSetLock(majorVersion, minorVersion)
                return
            }
        }

        recipeMap['osp'] = ospMap
        sharedLib.writeToReleaseFile(majorVersion, minorVersion, recipeMap)

    }

}
