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
        versions = sharedLib.fetchMajorMinorOSVersion("signed-container-image")
        def repoDetails = cimsg.build.extra.image
        def containerImage = repoDetails.index.pull.find({x -> !(x.contains("sha"))})
        def repoUrl = repoDetails.yum_repourls.find({x -> x.contains("Tools")})
        composeUrl = repoUrl.split("work").find({x -> x.contains("RHCEPH-${versions.major_version}.${versions.minor_version}")})
        println "repo url : ${composeUrl}"
        cephVersion = sharedLib.fetchCephVersion(composeUrl)
        def fileName = "RHCEPH-${versions.major_version}.${versions.minor_version}.yaml"
        def content = sharedLib.readFromReleaseFile(versions.major_version, versions.minor_version)
        if(!content)
        {
            sharedLib.unSetLock(versions.major_version, versions.minor_version)
            currentBuild.result = 'ABORTED'
            error "File:${fileName} does not exist...."
        }
        if (content["rc"]) {
            def resp = sharedLib.compareCephVersion(content["rc"]["ceph-version"], cephVersion)
            if (resp != 0) {
                sharedLib.unSetLock(versions.major_version, versions.minor_version)
                currentBuild.result = 'ABORTED'
                error "Higher version of ceph already exist...." }
            if (content["rc"]["repository"]) {
            content["rc"]["repository"] = containerImage
            }
            else{
            def repository = ["repository" : containerImage]
            content["rc"] += repository
            }
            }
        else {
                sharedLib.unSetLock(versions.major_version, versions.minor_version)
                currentBuild.result = 'ABORTED'
                error "No RC Contents present in yaml file"
                }
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
