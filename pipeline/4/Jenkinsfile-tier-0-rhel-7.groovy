/*
    Pipeline script for executing Tier 0 test suites for RH Ceph 4.x.
*/
// Global variables section
def nodeName = "centos-7"
def cephVersion = "nautilus"
def sharedLib
def test_results = [:]

// Pipeline script entry point
node(nodeName) {

    timeout(unit: "MINUTES", time: 30) {
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
            script {
                sharedLib = load("${env.WORKSPACE}/pipeline/vars/common.groovy")
                sharedLib.prepareNode()
            }
        }
    }

    timeout(unit: "HOURS", time: 7) {
  	    stage('RPM Sanity') {
		script {
		    withEnv([
			"osVersion=RHEL-7",
			"sutVMConf=conf/inventory/rhel-7.9-server-x86_64.yaml",
			"sutConf=conf/${cephVersion}/ansible/sanity-ceph-ansible.yaml",
			"testSuite=suites/${cephVersion}/ansible/sanity_ceph_ansible.yaml",
			"containerized=false",
			"addnArgs=--post-results --log-level DEBUG"
		    ]) {
			rc = sharedLib.runTestSuite()
			test_results['rpm_sanity'] = rc
		    }
		}
	    }
    }

    stage('Publish Results') {
        script {
            sharedLib.sendEMail("Tier-0", test_results)
            sharedLib.postLatestCompose()
        }
    }
}
