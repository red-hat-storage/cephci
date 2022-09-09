/*
    Pipeline script for executing weekly scheduled customer
*/
// file details
def bugzilla_api_file = "/ceph/report/bugzillarc"
def google_api_file = "/ceph/report/google_api_secret.json"
def dest_bugzilla_api_path = "/home/jenkins-build/.config/python-bugzilla"
def dest_google_api_path = "/home/jenkins-build/.gapi"

// Pipeline script entry point
node("rhel-8-medium || ceph-qe-ci") {
    stage('prepareNode') {
        if (env.WORKSPACE) { sh script: "sudo rm -rf * .venv" }
        checkout(
            scm: [
                $class: 'GitSCM',
                branches: [[name: 'main']],
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
                    url: 'https://gitlab.cee.redhat.com/rhcs-qe/reporter-bugzilla.git'
                ]]
            ],
            changelog: false,
            poll: false
        )
    }

stage("executeWorkflow") {
    println "Install packages"
    sh (script: "rm -rf .venv")
    sh (script: "python3 -m venv .venv")
    sh (script: ".venv/bin/python3 -m pip install --upgrade pip")
    sh (script: ".venv/bin/python3 -m pip install python-bugzilla gspread oauth2client pyyaml jinja_markdown pytz")

    println "Adding API files"
    sh (script : "mkdir -p ${dest_bugzilla_api_path}")
    sh (script : "cp ${bugzilla_api_file} ${dest_bugzilla_api_path}")

    sh (script : "mkdir -p ${dest_google_api_path}")
    sh (script : "cp ${google_api_file} ${dest_google_api_path}")

    println "Creating Customer Exp Report"
    sh (script: '.venv/bin/python3 customer_exp_report.py "ce_bugzilla_spreadsheet"')

    }
}

