/*
    Pipeline script for signed/rc compose listener
*/
// Global variables section
def nodeName = "centos-7"
def sharedLib
def versions
def cephVersion
def composeUrl

// Pipeline script entry point
node(nodeName) {
    timeout(unit: "MINUTES", time: 30) {
        stage('Install prereq') {
            if (env.WORKSPACE) {
                deleteDir() }
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
                    trackingSubmodules: false]],
                submoduleCfg: [],
                userRemoteConfigs: [[
                    url: 'https://github.com/red-hat-storage/cephci.git']]
            ])
            sharedLib = load("${env.WORKSPACE}/pipeline/vars/lib.groovy")
            sharedLib.prepareNode()
        }
    }

    stage("Update Release File") {
        def cimsg = sharedLib.getCIMessageMap()
        versions = sharedLib.fetchMajorMinorOSVersion("signed-compose")
        def composePath = cimsg["compose-path"].replace("/mnt/redhat/", "")
        composeUrl = "http://download-node-02.eng.bos.redhat.com/${composePath}".toString()
        cephVersion = sharedLib.fetchCephVersion(composeUrl)
        def location = "/ceph/cephci-jenkins/latest-rhceph-container-info"
        def fileName = "RHCEPH-${versions.major_version}.${versions.minor_version}.yaml"
        def fileExists = sh (returnStatus: true, script: "ls -l ${location}/${fileName}")
        if (fileExists != 0) {
            currentBuild.result = 'ABORTED'
            error "File:${fileName} does not exist...." }
        def content = sharedLib.readFromReleaseFile(versions.major_version, versions.minor_version)
        if (content["rc"]) {
            def resp = sharedLib.compareCephVersion(content["rc"]["ceph-version"], cephVersion)
            if (resp == -1) {
                sharedLib.unSetLock(versions.major_version, versions.minor_version)
                currentBuild.result = 'ABORTED'
                error "Higher version of ceph already exist...." }
            content["rc"]["ceph-version"] = cephVersion
            content["rc"]["compose-label"] = cimsg["compose-label"]
            if (content["rc"]["composes"][versions["platform"]]) {
                content["rc"]["composes"][versions["platform"]] = composeUrl }
            else {
                def platform = ["${versions.platform}": composeUrl]
                content["rc"]["composes"] += platform }
        }
        else {
            def updateContent = [
                "rc": [
                    "ceph-version": cephVersion,
                    "compose-label": cimsg["compose-label"],
                    "composes": [
                        "${versions.platform}": composeUrl]]]
            content += updateContent }
        sharedLib.writeToReleaseFile(versions.major_version, versions.minor_version, content)
        println "Content Updated To FILE:${fileName} Successfully"
    }

    stage('Publish UMB') {
        def overrideTopic = "VirtualTopic.qe.ci.rhcephqe.product-build.promote.complete"
        println "Update UMB Message In Topic:${overrideTopic}"
        def contentMap = [
            "artifact": [
                "build_action": "rc",
                "name": "Red Hat Ceph Storage",
                "nvr": "RHCEPH-${versions.major_version}.${versions.minor_version}",
                "phase": "rc",
                "type": "product-build",
                "version": cephVersion],
            "build": [
                "compose-url": composeUrl],
            "contact": [
                "email": "ceph-qe@redhat.com",
                "name": "Downstream Ceph QE"],
            "run": [
                "log": "${env.BUILD_URL}console",
                "url": env.BUILD_URL],
            "version": "1.0.0"]
        def msgContent = writeJSON returnText: true, json: contentMap
        sharedLib.SendUMBMessage(msgContent, overrideTopic, "ProductBuildDone")
        println "Updated UMB Message Successfully"
    }
}
