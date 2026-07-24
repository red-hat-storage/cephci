/* Library module that contains methods to send UMB message. */

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
            "latest": recipeMap['latest'],
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
