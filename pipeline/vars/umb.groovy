/* Library module that contains methods to send UMB message. */

def postUMBForPipeline(
    def rhcephVersion,
    def cephVersion,
    def buildArtifacts,
    def tierLevel,
    def tags,
    def overrides,
    def runType,
    def buildType,
    def result,
    def finalStage = false,
    def dirName = null,
){
    /*
    */
    def msgMap = [
        "artifact": [
            "type": "product-build",
            "name": "Red Hat Ceph Storage",
            "version": cephVersion,
            "nvr": rhcephVersion,
            "phase": "testing",
            "build": "tier-0",
        ],
        "contact": [
            "name": "Downstream Ceph QE",
            "email": "cephci@redhat.com",
        ],
        "system": [
            "os": "centos-7",
            "label": "agent-01",
            "provider": "IBM-Cloud",
        ],
        "pipeline": [
            "name": tierLevel,
            "id": currentBuild.number,
            "tags": tags,
            "overrides": overrides,
            "run_type": runType,
            "final_stage": finalStage,
        ],
        "run": [
            "url": env.RUN_DISPLAY_URL,
            "additional_urls": [
                "doc": "https://docs.engineering.redhat.com/display/rhcsqe/RHCS+QE+Pipeline",
                "repo": "https://github.com/red-hat-storage/cephci",
                "report": "https://reportportal-rhcephqe.apps.ocp4.prod.psi.redhat.com/",
                "tcms": "https://polarion.engineering.redhat.com/polarion/",
            ],
        ],
        "test": [
            "type": buildType,
            "category": "functional",
            "result": result,
            "object-prefix": dirName,
        ],
        "recipe": buildArtifacts,
        "generated_at": env.BUILD_ID,
        "version": "3.0.0",
    ]
    def msgType = "Custom"

    sharedLib.SendUMBMessage(
        msgMap,
        "VirtualTopic.qe.ci.rhcephqe.product-build.test.complete",
        msgType,
    )
}

def postUMBTestQueue(def version, Map recipeMap, def scratch) {
    /*
        This method posts a message to the UMB using the version information provided.

        Args:
            version (str):  Version information to be included in the payload.
            image (str):    Ceph image
    */
    def stableBuild = ( recipeMap.containsKey('osp') ) ? recipeMap['osp'] : [:]
    def payload = [
        "artifact": [
            "nvr": version,
            "latest": recipeMap['tier-0'],
            "stable": stableBuild,
            "scratch": scratch
        ],
        "category": "external",
        "contact": [
            "email": "cephci@redhat.com",
            "name": "Red Hat Ceph Storage QE Team"
        ],
        "issuer": "rhceph-qe-ci",
        "run": [
            "url": "${env.JENKINS_URL}/job/${env.JOB_NAME}/${env.BUILD_NUMBER}/",
            "log": "${env.JENKINS_URL}/job/${env.JOB_NAME}/${env.BUILD_NUMBER}/console"
        ],
        "version": "1.0.0"
    ]

    def msg = writeJSON returnText: true, json: payload
    def msgProps = """ PRODUCT = Red Hat Ceph Storage
        TOOL = cephci
    """
    sendCIMessage([
        providerName: 'Red Hat UMB',
        overrides:[topic: 'VirtualTopic.qe.ci.rhceph.product-scenario.test.queue'],
        messageContent: msg,
        messageProperties: msgProps,
        messageType: "Custom",
        failOnError: true
    ])
}

return this;
