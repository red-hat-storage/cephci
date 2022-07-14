"""Module that executes workflows required for network node"""
from utility import lvm_utils
from utility.log import Log
from utility.utils import Log, get_cephci_config
from install_prereq import *
log = Log(__name__)


def run(ceph_cluster, **kw):
    log.info("Running test")
    ceph_nodes = kw.get("ceph_nodes")
    # skip subscription manager if testing beta RHEL
    config = kw.get("config")

    # Install prerequisite on Network node to setup local registry
    for ceph in ceph_nodes:
        if ceph.role == "network-node":
            # Subscribe channels and install packages to setup up network Node
            install_prereq(ceph)
            ceph.exec_command(
                cmd="sudo yum install -y httpd-tools httpd createrepo",
                long_running=True,
            )
            ceph.exec_command(sudo=True, cmd="mkdir -p /opt/registry/{auth,certs,data}")

            # Create credentials for accessing the private registry
            ceph.exec_command(
                cmd="sudo htpasswd -bBc /opt/registry/auth/htpasswd \
                     myregistryusername myregistrypassword1"
            )

            # Create a self-signed certificate
            ceph.exec_command(
                cmd=f'(echo "."; echo ".";echo ".";echo ".";echo ".";\
                        echo "{ceph.hostname}";echo ".";)| sudo openssl req -newkey \
                        rsa:4096 -nodes -sha256 -keyout /opt/registry/certs/domain.key \
                        -x509 -days 365 -out /opt/registry/certs/domain.crt -addext \
                        "subjectAltName = DNS:{ceph.hostname}"'
            )

            # Create a symbolic link to domain.cert
            ceph.exec_command(
                cmd="sudo ln -s /opt/registry/certs/domain.crt \
                    /opt/registry/certs/domain.cert"
            )

            # Add certificate to trusted list on the private registry node
            ceph.exec_command(
                cmd="sudo cp /opt/registry/certs/domain.crt \
                    /etc/pki/ca-trust/source/anchors/"
            )
            ceph.exec_command(cmd="sudo update-ca-trust")
            update_cert(ceph)

            # Perform passwordless ssh between all nodes
            ceph_cluster.setup_ssh_keys()
            file_name = "/opt/registry/certs/domain.crt"

            # Copy the certificate to other nodes
            network_node = ceph_nodes.get_ceph_object("network-node").node
            for ceph in ceph_nodes:
                if ceph.role != "network-node":
                    ceph.exec_command(
                        cmd="sudo touch /etc/pki/ca-trust/source/anchors/domain.crt"
                    )
                    contents, _ = network_node.exec_command(
                        sudo=True, cmd="cat /opt/registry/certs/domain.crt"
                    )
                    key_file = ceph.remote_file(
                        sudo=True,
                        file_name="/etc/pki/ca-trust/source/anchors/domain.crt",
                        file_mode="a",
                    )
                    key_file.write(contents)
                    key_file.flush()
                    ceph.exec_command(cmd="sudo update-ca-trust")
                    update_cert(network_node)

            # Start Private registry on Network Node
            start_private_registry(network_node)

            # Add images to private registry
            private_registry_add_image(network_node, config)

            # Generate repository file
            repos = ["Tools"]
            base_url = config.get("base_url")
            ceph_repo = ceph_cluster.generate_repository_file(base_url, repos)
            repo_file = network_node.remote_file(
                sudo=True, file_name="/etc/yum.repos.d/rh_ceph.repo", file_mode="w"
            )
            repo_file.write(ceph_repo)
            repo_file.flush()

            # Setup local yum repo server on Network Node
            local_yum_repository_server(network_node)

            # Create local repo on other nodes in cluster
            create_local_repo(network_node, ceph_nodes)

    return 0


def update_cert(ceph):
    # Check if the certificate is updated on all other nodes in cluster

    out, _ = ceph.exec_command(cmd=f"sudo trust list | grep -i {ceph.hostname}")
    if ceph.hostname in out:
        log.info("Certificate added to network node successfully")
    else:
        log.error("Certificate failed to add in network node")
    return 1


def start_private_registry(ceph):
    # Create and start the pricate registry on port 5000

    ceph.exec_command(
        cmd="sudo podman run --restart=always --name myprivateregistry \
                    -p 5000:5000 -v /opt/registry/data:/var/lib/registry:z \
                    -v /opt/registry/auth:/auth:z \
                    -v /opt/registry/certs:/certs:z \
                    -e 'REGISTRY_AUTH=htpasswd' \
                    -e 'REGISTRY_AUTH_HTPASSWD_REALM=Registry Realm' \
                    -e REGISTRY_AUTH_HTPASSWD_PATH=/auth/htpasswd \
                    -e 'REGISTRY_HTTP_TLS_CERTIFICATE=/certs/domain.crt' \
                    -e 'REGISTRY_HTTP_TLS_KEY=/certs/domain.key' \
                    -e REGISTRY_COMPATIBILITY_SCHEMA1_ENABLED=true \
                    -d registry:2"
    )
    log.info("Private Registry Started")


def create_images_dict(config):
    # Return a dictionary with key as image name and value as image path
    # Retun a list of images

    image = []
    supported_overrides = [
        "node_exporter",
        "grafana",
        "prometheus",
        "alertmanager",
    ]

    # Fetch images from yaml file
    node_exporter = config.get("ansi_config").get("node_exporter_container_image")
    grafana_container_image = config.get("ansi_config").get("grafana_container_image")
    prometheus_container_image = config.get("ansi_config").get(
        "prometheus_container_image"
    )
    alertmanager_container_image = config.get("ansi_config").get(
        "alertmanager_container_image"
    )

    # Add images to a list
    image.append(node_exporter)
    image.append(grafana_container_image)
    image.append(prometheus_container_image)
    image.append(alertmanager_container_image)

    # Create dictionary of images
    image_dict = {
        supported_overrides[i]: image[i] for i in range(len(supported_overrides))
    }
    return (image_dict, image)


def private_registry_add_image(ceph, config):
    image_dict, image = create_images_dict(config)
    config_ = get_cephci_config()
    container_image = config.get("container_image")
    username_ = config_["cdn_credentials"]["username"]
    password_ = config_["cdn_credentials"]["password"]

    # Adding node_exporter,grafana_container_image,
    # prometheus_container_image,alertmanager_container_image
    for img in image:
        ceph.exec_command(
            cmd=f"sudo podman run -v /opt/registry/certs:/certs:Z \
                        -v /opt/registry/certs/domain.cert:/certs/domain.cert:Z \
                        --rm registry.redhat.io/rhel8/skopeo:8.5-8 skopeo copy  \
                        --remove-signatures --src-creds {username_}:{password_} \
                        --dest-cert-dir=./certs/ --dest-creds \
                        myregistryusername:myregistrypassword1 \
                        docker://{img} \
                        docker://{ceph.hostname}:5000/{img} --tls-verify=false",
            timeout=1200,
        )

    # Adding RedHat ceph image to private registry
    ceph.exec_command(
        cmd=f"sudo podman run -v /opt/registry/certs:/certs:Z \
                        -v /opt/registry/certs/domain.cert:/certs/domain.cert:Z \
                        --rm registry.redhat.io/rhel8/skopeo:8.5-8 skopeo copy  \
                        --remove-signatures \
                        --dest-cert-dir=./certs/ --dest-creds \
                        myregistryusername:myregistrypassword1 \
                        docker://{container_image} \
                        docker://{ceph.hostname}:5000/testm/testm1 \
                        --tls-verify=false",
        timeout=1200,
    )

    # List images in private registry
    ceph.exec_command(
        cmd=f"curl -u myregistryusername:myregistrypassword1 \
                https://{ceph.hostname}:5000/v2/_catalog"
    )


def local_yum_repository_server(ceph):
    ceph.exec_command(cmd="sudo mkdir -p /var/www/html/repo")

    # Attach disk and mount it to /var/www/html folder
    disk_attach_http(ceph)

    # Copy the packages to /var/www/html location
    rhel_list = [
        "rhel-8-for-x86_64-appstream-rpms",
        "rhel-8-for-x86_64-baseos-rpms",
        "ceph-Tools",
    ]
    for repo in rhel_list:
        ceph.exec_command(
            cmd=f"(sudo reposync --repo {repo} -p /var/www/html/repo/ \
                    --download-metadata --downloadcomps)",
            long_running=True,
        )
        ceph.exec_command(
            cmd=f"(sudo createrepo -v /var/www/html/repo/{repo}/ -g comps.xml)",
            long_running=True,
        )

    # Edit the http.conf file
    ceph.exec_command(
        cmd=f"sudo sed -i 's/root@localhost/root@{ceph.hostname}/' \
                /etc/httpd/conf/httpd.conf"
    )
    ceph.exec_command(
        cmd=f"sudo sed -i '/#ServerName/c\ServerName {ceph.hostname}' \
                /etc/httpd/conf/httpd.conf"
    )

    # Start http service
    ceph.exec_command(sudo=True, cmd="systemctl start httpd.service")
    log.info("http Service restarted!!")
    ceph.exec_command(sudo=True, cmd="systemctl enable httpd.service")

    # Set selinux label for http
    ceph.exec_command(cmd="sudo restorecon -R /var/www/html/")
    ceph.exec_command(
        cmd="sudo semanage fcontext -a -t httpd_sys_rw_content_t /var/www/html/"
    )


def create_local_repo(network_node, ceph_nodes):
    # Create local repo on other servers
    rhel_list = [
        "rhel-8-for-x86_64-appstream-rpms",
        "rhel-8-for-x86_64-baseos-rpms",
        "ceph-Tools",
    ]
    for ceph in ceph_nodes:
        repo_file = ""
        if ceph.role != "network-node":
            for i in rhel_list:
                base_url = "http://" + network_node.hostname + "/repo/" + i
                log.info("Using %s", base_url)
                header = "[" + i + "]" + "\n"
                name = "name=" + i + "\n"
                baseurl = "baseurl=" + base_url + "\n"
                gpgcheck = "gpgcheck=0\n"
                enabled = "enabled=1\n\n"
                repo_file = repo_file + header + name + baseurl + gpgcheck + enabled
        local_file = ceph.remote_file(
            sudo=True, file_name="/etc/yum.repos.d/local.repo", file_mode="w"
        )
        local_file.write(repo_file)
        local_file.flush()


def disk_attach_http(ceph):
    # Create pv,vg,lv and format it with xfs filesystem
    dev = "/dev/vdb"
    log.info("creating pv on %s" % ceph.hostname)
    lvm_utils.pvcreate(ceph, dev)
    log.info("creating vg1  %s" % ceph.hostname)
    lvm_utils.vgcreate(ceph, "vg1", dev)
    log.info("creating lv1 %s" % ceph.hostname)
    ceph.exec_command(cmd="sudo lvcreate -L 200G -n lv1 vg1")
    ceph.exec_command(cmd="sudo mkfs.xfs /dev/mapper/vg1-lv1")
    ceph.exec_command(cmd="sudo mount /dev/mapper/vg1-lv1 /var/www/html")
