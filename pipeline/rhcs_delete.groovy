/*
    Script to remove an existing cluster via UMB message.
*/
def sharedLib

node("centos-7") {

    timeout(unit: "MINUTES", time: 30) {
        stage("Prepare env") {
            if (env.WORKSPACE) {
                sh (script: "sudo rm -rf *")
            }

            checkout ([
                $class: 'GitSCM',
                branches: [[name: 'origin/master']],
                doGenerateSubmoduleConfigurations: false,
                extensions: [[
                    $class: 'CloneOption',
                    shallow: true,
                    noTags: true,
                    reference: '',
                    depth: 0
                ]],
                submoduleCfg: [],
                userRemoteConfigs: [[
                    url: 'https://github.com/red-hat-storage/cephci.git'
                ]]
            ])

            sharedLib = load("${env.WORKSPACE}/pipeline/vars/lib.groovy")
            sharedLib.prepareNode()
        }
    }

    stage("Remove cluster") {
        def ciMap = sharedLib.getCIMessageMap()

        def cmd = ".venv/bin/python run.py --log-level debug"
        cmd += " --osp-cred ${env.HOME}/osp-cred-ci-2.yaml"
        cmd += " --cleanup ${ciMap['instances-name']}"

        println ("Command: ${cmd}")

        sh (script: cmd)
        println "Successfully removed the cluster."
    }

}
