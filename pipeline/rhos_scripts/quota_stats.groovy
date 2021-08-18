#!/usr/bin/env groovy
/*
    Methods to fetch quota statistics from rhos-d.
*/

def installOpenStackClient(){
    sh("source .venv/bin/activate; python -m pip install python-openstackclient")
}

def fetchBaseCommand(args){
    osp_cred_file = args['osp-cred']
    project = args['project']
    Map osp_cred = readYaml file: osp_cred_file
    osp_glbs = osp_cred["globals"]
    os_cred = osp_glbs["openstack-credentials"]

    os_auth_url = "${os_cred["auth-url"]}/v3"
    os_base_cmd = "source .venv/bin/activate; "
    os_base_cmd += "openstack --os-auth-url ${os_auth_url}"
    os_base_cmd += " --os-project-domain-name ${os_cred["domain"]}"
    os_base_cmd += " --os-user-domain-name ${os_cred["domain"]}"
    os_base_cmd += " --os-project-name ${project}"
    os_base_cmd += " --os-username ${os_cred["username"]}"
    os_base_cmd += " --os-password ${os_cred["password"]}"
    os_base_cmd = "set +x && ${os_base_cmd}"
    return os_base_cmd
}

def fetch_instance_usage(os_base_cmd){
    os_usage_cmd = "${os_base_cmd} usage show -f json"
    // fetch memory usage stats for all instances
    os_usage = sh(returnStdout: true, script: os_usage_cmd).trim()
    os_usage_json = readJSON text: os_usage
    os_instances_usage = os_usage_json["Servers"]
    return os_instances_usage
}

def fetch_instances_project(os_base_cmd){
    os_node_cmd = "${os_base_cmd} server list -f json"
    // fetch all instances created for the project
    os_nodes = sh(returnStdout: true, script: os_node_cmd)
    os_nodes_json = readJSON text: os_nodes
    return os_nodes_json
}

def fetchInstanceDetail(os_base_cmd, instance_id){
    def os_node_detail_cmd = "${os_base_cmd} server show ${instance_id} -f json"
    def os_node_detail = sh(returnStdout: true, script: os_node_detail_cmd).toString()
    os_node_detail_json = readJSON text: os_node_detail
    return os_node_detail_json
}

def fetchUserDetail(os_base_cmd, user_id){
    os_user_cmd = "${os_base_cmd} user show ${user_id} -f json"
    os_user = sh(returnStdout: true, script: os_user_cmd).trim()
    os_user_json = readJSON text: os_user
    return os_user_json
}

def fetchUsageForInstance(os_base_cmd, state, flavor){
    Map usage = ['memory': 0, 'vcpus': 0, 'volumestorage': 0]
    if(state == "ACTIVE"){
        os_instance_usage_cmd = "${os_base_cmd} flavor show -c ram -c vcpus -c disk ${flavor} -f json"
        def os_instance_usage_detail = sh(returnStdout: true, script: os_instance_usage_cmd).toString()
        os_instance_usage_detail_json = readJSON text: os_instance_usage_detail
        usage.memory = os_instance_usage_detail_json["ram"]
        usage.vcpus = os_instance_usage_detail_json["vcpus"]
        usage.volumestorage = os_instance_usage_detail_json["disk"]
    }
    return usage
}

def fetchInstanceMapForUser(os_base_cmd, os_nodes){
    def is_usage_available = false
    try{
        os_instances_usage = fetch_instance_usage(os_base_cmd)
        is_usage_available = true
    } catch(Exception ex){
        echo "Exception while fetching usage"
        echo ex.getMessage()
        is_usage_available = false
    }
    instance_detail = [:]
    user_detail = [:]

    for (instance in os_nodes){
        instance_id = instance["ID"]

        println "Fetching node detail for instance " + instance_id
        try {
            def node_detail_json = fetchInstanceDetail(os_base_cmd, instance_id)
            user_id = node_detail_json["user_id"]
            state = node_detail_json["status"]
            flavor = node_detail_json["flavor"].toString().split()[0]
            user_name = ""
            if (user_detail.containsKey(user_id)){
                user_name = user_detail[user_id]
            }
            else{
                os_user_json = fetchUserDetail(os_base_cmd, user_id)
                user_name = os_user_json["name"]
                user_detail.put(user_id, user_name)
            }


            usage = fetchUsageForInstance(os_base_cmd, state, flavor)

            if (instance_detail.containsKey(user_name)){
                instance_detail[user_name]["Instances"].add(instance["Name"])
                instance_detail[user_name]["Instance States"].add(state)
                instance_detail[user_name]["RAM Used Per Instance in MB"].add(usage.memory)
                instance_detail[user_name]["VCPUs Used Per Instance"].add(usage.vcpus)
                instance_detail[user_name]["Volume Used Per Instance in GB"].add(usage.volumestorage)
            }
            else{
                instance_dict = [
                        "Instances": [instance["Name"]],
                        "Instance States": [state],
                        "RAM Used Per Instance in MB": [usage.memory],
                        "VCPUs Used Per Instance": [usage.vcpus],
                        "Volume Used Per Instance in GB": [usage.volumestorage]
                    ]
                instance_detail.put(user_name, instance_dict)
            }
        } catch(Exception ex){
            echo "Exception"
            echo ex.getMessage()
            continue
        }
    }
    return instance_detail
}

def fetch_complete_quota(os_base_cmd, instance_detail, project_name){
    total_instances_used = 0
    total_vcpus_used = 0
    total_ram_used = 0
    total_volume_used = 0
    def user_stats
    for (k in instance_detail.keySet()){
        user = k
        def stat_map = [
                "User" : user,
                "Project" : project_name,
                "Instance Count" : instance_detail[user]["Instances"].size(),
                "RAM Used in GB" : instance_detail[user]["RAM Used Per Instance in MB"].sum()/1024,
                "VCPU Used" : instance_detail[user]["VCPUs Used Per Instance"].sum(),
                "Volume Used in GB" : instance_detail[user]["Volume Used Per Instance in GB"].sum()
            ]
        if(user_stats){
            user_stats.add(stat_map)
        }
        else{
            user_stats = [stat_map]
        }
        total_instances_used += stat_map["Instance Count"]
        total_ram_used += stat_map["RAM Used in GB"]
        total_vcpus_used += stat_map["VCPU Used"]
        total_volume_used += stat_map["Volume Used in GB"]
    }

    os_quota_cmd = "${os_base_cmd} quota show -f json"
    os_quota = sh(returnStdout: true, script: os_quota_cmd).trim()
    os_quota_json = readJSON text: os_quota
    def ram_percent = (total_ram_used * 100)/(os_quota_json["ram"]/1024)
    ram_percent = ram_percent.floatValue().round(2)
    def vcpu_percent = (total_vcpus_used * 100)/os_quota_json["cores"]
    vcpu_percent = vcpu_percent.floatValue().round(2)
    def storage_percent = (total_volume_used * 100)/(os_quota_json["gigabytes"])
    storage_percent = storage_percent.floatValue().round(2)


    quota_usage_dict = [
        'Project Name' : project_name,
        'RAM usage in %' : ram_percent,
        'VCPU usage in %' : vcpu_percent,
        'Storage usage in %' : storage_percent
    ]

    quota_stats = [
        'project_stats': quota_usage_dict,
        'user_stats': user_stats
    ]

    return quota_stats
}

def fetch_quota_for_project(args){
    os_base_cmd = fetchBaseCommand(args)

    os_nodes = fetch_instances_project(os_base_cmd)
    instance_detail = fetchInstanceMapForUser(os_base_cmd, os_nodes)
    quota_stats = fetch_complete_quota(os_base_cmd, instance_detail, args["project"])
    return quota_stats
}

@NonCPS
def sortMapWithKey(List<Map> list, def key, def order){
    if(order == "ascending"){
        list.sort{ a, b -> a[key] <=> b[key]}
    }
    else if(order == "descending"){
        list.sort{ a, b -> b[key] <=> a[key]}
    }
}

def fetch_quota(args){
    projects = args["projects"]
    def overall_project_stats = []
    def overall_user_stats = []
    def quota_detail
    for (project in projects){
        proj_args = ["osp-cred" : args["osp-cred"],
                    "project": project]
        def proj_quota_detail = fetch_quota_for_project(proj_args)
        if(quota_detail){
            quota_detail["Project Stats"].add(proj_quota_detail["project_stats"])
            quota_detail["User Stats"].addAll(proj_quota_detail["user_stats"])
        }
        else{
            quota_detail = [
                    "Project Stats" : [proj_quota_detail["project_stats"]],
                    "User Stats" : proj_quota_detail["user_stats"]
                ]
        }
    }
    def proj_stats_sorted = sortMapWithKey(quota_detail["Project Stats"], "RAM usage in %", "descending")
    quota_detail["Project Stats"] = proj_stats_sorted
    def user_stats_sorted = sortMapWithKey(quota_detail["User Stats"], "Instance Count", "descending")
    quota_detail["User Stats"] = user_stats_sorted
    return quota_detail
}

@NonCPS
def generate_mail_body(html_body, quota_detail){
    /*
        Generate mail body using groovy template for sending mail.
    */
    def data = [quota : quota_detail, pro_range : [80, 50], user_range : [20, 10]]
    def mail_body = new groovy.text.StreamingTemplateEngine().createTemplate(html_body).make(data)
    return mail_body.toString()
}

def sendEmail(def quota_detail){
     /*
        Send an email notification.
    */
    echo "Sending Email"
    def html_body = readFile(file: "pipeline/rhos_scripts/quota-template.html")
    def body = generate_mail_body(html_body, quota_detail)
    def to_list = "cephci@redhat.com, ceph-qe@redhat.com"

    emailext(
        mimeType: 'text/html',
        subject: "Quota Usage Statistics for rhos-d projects.",
        body: "${body}",
        from: "cephci@redhat.com",
        to: "${to_list}"
    )
}

return this;
