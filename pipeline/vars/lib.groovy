#!/usr/bin/env groovy
/*
    Common groovy methods that can be reused by the pipeline jobs.
*/

import org.jsoup.Jsoup

def yamlToMap(def yamlFile, def location="/ceph/cephci-jenkins/latest-rhceph-container-info") {
    /*
        Read the yaml file and returns a map object
    */
    def yamlfileExists = sh (returnStatus: true, script: "ls -l ${location}/${yamlFile}")
    if (yamlfileExists != 0) {
        println "File ${location}/${yamlFile} does not exist."
        return [:]
    }
    def props = readYaml file: "${location}/${yamlFile}"
    return props
}

def getCIMessageMap() {
    /*
        Return the CI_MESSAGE map
    */
    def ciMessage = "${params.CI_MESSAGE}" ?: ""
    if (! ciMessage?.trim() ) {
        return [:]
    }
    def compose = readJSON text: "${params.CI_MESSAGE}"
    return compose
}

def fetchMajorMinorOSVersion(def build_type){
    /*
        method accepts build_type as an input and
        Returns RH-CEPH major version, minor version and OS platform based on build_type
        different build_type supported: unsigned-compose, unsigned-container-image, cvp, signed-compose, signed-container-image

    */
    def cimsg = getCIMessageMap()
    def major_ver
    def minor_ver
    def platform

    if (build_type == 'unsigned-compose' || build_type == 'unsigned-container-image') {
        major_ver = cimsg.compose_id.substring(7,8)
        minor_ver = cimsg.compose_id.substring(9,10)
        platform = cimsg.compose_id.substring(11,17).toLowerCase()
    }
    if (build_type == 'cvp'){
        major_ver = cimsg.artifact.brew_build_target.substring(5,6)
        minor_ver = cimsg.artifact.brew_build_target.substring(7,8)
        platform = cimsg.artifact.brew_build_target.substring(9,15).toLowerCase()
    }
    if (build_type == 'signed-compose'){
        major_ver = cimsg["compose-id"].substring(7,8)
        minor_ver = cimsg["compose-id"].substring(9,10)
        platform = cimsg["compose-id"].substring(11,17).toLowerCase()
    }
    if (build_type == 'signed-container-image'){
        major_ver = cimsg.tag.name.substring(5,6)
        minor_ver = cimsg.tag.name.substring(7,8)
        platform = cimsg.tag.name.substring(9,15).toLowerCase()
    }
    if (major_ver && minor_ver && platform){
        return ["major_version":major_ver, "minor_version":minor_ver, "platform":platform]
    }
    error "Required values are not obtained.."
}

def fetchCephVersion(def base_url){
    /*
        Fetches ceph version using compose base url
    */
    base_url += "/compose/Tools/x86_64/os/Packages/"
    println base_url
    def document = Jsoup.connect(base_url).get().toString()
    def ceph_ver = document.findAll(/"ceph-common-([\w.-]+)\.([\w.-]+)"/)[0].findAll(/([\d]+)\.([\d]+)\.([\d]+)\-([\d]+)/)
    println ceph_ver
    if (! ceph_ver){
        error "ceph version not found.."
    }
    return ceph_ver[0]
}

def setLock(def major_ver, def minor_ver){
    /*
        create a lock file
    */
    def defaultFileDir = "/ceph/cephci-jenkins/latest-rhceph-container-info"
    def lock_file = "${defaultFileDir}/RHCEPH-${major_ver}.${minor_ver}.lock"
    def lockFileExists = sh (returnStatus: true, script: "ls -l ${lock_file}")
    if (lockFileExists != 0) {
        println "RHCEPH-${major_ver}.${minor_ver}.lock does not exist. creating it"
        sh(script: "touch ${lock_file}")
        return
    }
    def startTime = System.currentTimeMillis()
    while((System.currentTimeMillis()-startTime)<600000){
        lockFilePresent = sh (returnStatus: true, script: "ls -l ${lock_file}")
        if (lockFilePresent != 0) {
            sh(script: "touch ${lock_file}")
            return
            }
    }
    error "Lock file: RHCEPH-${major_ver}.${minor_ver}.lock already exist.can not create lock file"
}

def unSetLock(def major_ver, def minor_ver){
    /*
        Unset a lock file
    */
    def defaultFileDir = "/ceph/cephci-jenkins/latest-rhceph-container-info"
    def lock_file = "${defaultFileDir}/RHCEPH-${major_ver}.${minor_ver}.lock"
    sh(script: "rm -f ${lock_file}")
}
