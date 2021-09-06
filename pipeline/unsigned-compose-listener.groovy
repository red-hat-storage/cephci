/*
    Pipeline script for modifying the unsigned RPM information
*/
// Global variables section
def nodeName = "centos-7"
def sharedLib
def cephVersion
def composeUrl

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
                userRemoteConfigs: [[
                    url: 'https://github.com/red-hat-storage/cephci.git'
                ]]
            ])
            sharedLib = load("${env.WORKSPACE}/pipeline/vars/lib.groovy")
            sharedLib.prepareNode(1)
        }
    }

    stage('Update Release Yaml'){
        echo "${params.CI_MESSAGE}"

        ciMsg = sharedLib.getCIMessageMap()
        composeUrl = ciMsg["compose_url"]

        cephVersion = sharedLib.fetchCephVersion(composeUrl)

        versionInfo = sharedLib.fetchMajorMinorOSVersion("unsigned-compose")
        majorVer = versionInfo['major_version']
        minorVer = versionInfo['minor_version']
        platform = versionInfo['platform']

        releaseContent = sharedLib.readFromReleaseFile(majorVer, minorVer)

        if(releaseContent.isEmpty() || !releaseContent.containsKey("latest")){
            Map latestMap = [
                                "ceph-version": cephVersion,
                                "composes":
                                [
                                    "${platform}": composeUrl
                                ]
                            ]
            releaseContent.put("latest", latestMap)
            sharedLib.writeToReleaseFile(majorVer, minorVer, releaseContent)
        }
        else if(releaseContent.containsKey("latest")){
            currentCephVersion = releaseContent["latest"]["ceph-version"]
            compare = sharedLib.compareCephVersion(currentCephVersion, cephVersion)

            if(compare == 0 && !releaseContent["latest"]["composes"].containsKey(platform)){
                releaseContent["latest"]["composes"].put(platform, composeUrl)
            }
            else if(compare == 1){
                Map latestMap = [
                                    "ceph-version": cephVersion,
                                    "composes":
                                    [
                                        "${platform}": composeUrl
                                    ]
                                ]
                releaseContent.put("latest", latestMap)
            }
            sharedLib.writeToReleaseFile(majorVer, minorVer, releaseContent)
        }
    }

    stage('Publish Unsigned RPM Compose') {

        def msgMap = [
            "artifact": [
                "type" : "unsigned-product-build",
                "name": "Red Hat Ceph Storage",
                "version": cephVersion,
                "nvr": "RHCEPH-${majorVer}.${minorVer}",
                "build_action": "latest",
                "phase": "tier-0"
            ],
            "contact":[
                "name": "Downstream Ceph QE",
                "email": "ceph-qe@redhat.com"
            ],
            "build": [
                "compose-url": composeUrl
            ],
            "run": [
                "url": env.BUILD_URL,
                "log": "${env.BUILD_URL}console"
            ],
            "version": "1.0.0"
        ]
        def topic = 'VirtualTopic.qe.ci.rhcephqe.product-build.promote.complete'

        //send UMB message notifying that a new build has arrived
        sharedLib.SendUMBMessage(msgMap, topic, 'ProductBuildDone')
    }

}
