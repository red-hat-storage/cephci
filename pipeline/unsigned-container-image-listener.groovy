/*
    Pipeline script for unsigned container image listener workflow
*/
// Global variables section
def nodeName = "centos-7"
def lib
def versions
def ceph_version
def compose

// Pipeline script entry point

node(nodeName) {

    timeout(unit: "MINUTES", time: 30) {
        stage('Install prereq') {
            checkout([
                $class: 'GitSCM',
                branches: [[name: '*/master']],
                doGenerateSubmoduleConfigurations: false,
                extensions: [[
                    $class: 'CloneOption',
                    shallow: true,
                    noTags: false,
                    reference: '',
                    depth: 0
                ]],
                submoduleCfg: [],
                userRemoteConfigs: [[
                    url: 'https://github.com/red-hat-storage/cephci.git'
                ]]
            ])

            // prepare the node
            lib = load("${env.WORKSPACE}/pipeline/vars/lib.groovy")
            lib.prepareNode(1)
        }
    }

    stage('Update Release Yaml') {
        println "msg = ${params.CI_MESSAGE}"
        compose = lib.getCIMessageMap()
        versions = lib.fetchMajorMinorOSVersion('unsigned-container-image')
        ceph_version = lib.fetchCephVersion(compose.compose_url)
        def fileName = "RHCEPH-${versions.major_version}.${versions.minor_version}.yaml"
        def releaseMap = lib.readFromReleaseFile(versions.major_version, versions.minor_version)
        println "releaseMap : " + releaseMap
        if (! releaseMap.latest) {
            lib.unSetLock(versions.major_version, versions.minor_version)
            error "'latest' key does not exist in the ${fileName} file"
        }
        if (lib.compareCephVersion(releaseMap.latest["ceph-version"] , ceph_version) !=0 ) {
            lib.unSetLock(versions.major_version, versions.minor_version)
            error "Ceph versions ${relMap.latest['ceph-version']} and ${ceph_version} mismatched"

        }
        println "Updating repository as ${versions['platform']} exists in ${fileName} latest map"
        releaseMap.latest.repository = compose.repository
        lib.writeToReleaseFile(versions.major_version, versions.minor_version, releaseMap)
    }

    stage('Publish UMB') {
        def artifactsMap = [
            "artifact": [
                "type": "product-build",
                "name": "Red Hat Ceph Storage",
                "version": ceph_version,
                "nvr": "RHCEPH-${versions.major_version}.${versions.minor_version}",
                "phase": "tier-0",
                "build_action": "latest"],
            "contact": [
                "name": "Downstream Ceph QE",
                "email": "ceph-qe@redhat.com"],
            "build": [
                "repository": compose.repository],
            "test": [
                "phase": "tier-0"],
            "run": [
                "url": "${env.BUILD_URL}",
                "log": "${env.BUILD_URL}/console"],
            "version": "1.0.0"]
        def msgContent = writeJSON returnText: true, json: artifactsMap
        println "${msgContent}"
        def overrideTopic = "VirtualTopic.qe.ci.rhcephqe.product-build.promote.complete"
        def msgType = "ProductBuildDone"
        lib.SendUMBMessage(msgContent, overrideTopic, msgType)
    }
}
