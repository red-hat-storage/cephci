/*
    Script to remove an existing cluster via UMB message.
*/
def sharedLib

node("rhel-8-medium || ceph-qe-ci") {

    stage("prepareNode") {
        if (env.WORKSPACE) { sh script: "sudo rm -rf * .venv" }

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

        sharedLib = load("${env.WORKSPACE}/pipeline/vars/v3.groovy")
        sharedLib.prepareNode()
    }

    stage("removeCluster") {
        def ciMap = sharedLib.getCIMessageMap()

        def cmd = ".venv/bin/python run.py --log-level debug"
        cmd += " --osp-cred ${env.HOME}/osp-cred-ci-2.yaml"
        cmd += " --cleanup ${ciMap['instances-name']}"

        println ("Command: ${cmd}")

        sh (script: cmd)
        println "Successfully removed the cluster."
    }

}
