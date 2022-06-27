import os


class BuildConfigForPrerequisite:

    def __init__(self, args, config, docker_registry, docker_image, docker_tag, osp_cred, base_url):
        self.args = args
        self.config = config
        self.docker_registry = docker_registry
        self.docker_image = docker_image
        self.docker_tag = docker_tag
        self.osp_cred = osp_cred
        self.base_url = base_url

    def build_config(self):
        if not self.config.get("base_url"):
            self.config["base_url"] = self.base_url
        rhbuild = self.args["--rhbuild"]
        platform = self.args["--platform"]
        self.config["rhbuild"] = f"{rhbuild}-{platform}"

        cloud_type = self.args.get("--cloud", "openstack")
        self.config["cloud-type"] = cloud_type
        ubuntu_repo = self.args.get("--ubuntu-repo", None)
        if "ubuntu_repo" in locals():
            self.config["ubuntu_repo"] = ubuntu_repo

        if self.args.get("--skip-cluster") is True:
            self.config["skip_setup"] = True

        if self.args.get("--skip-subscription") is True:
            self.config["skip_subscription"] = True

        if self.args.get("--add-repo"):
            repo = self.args.get("--add-repo")
            if repo.startswith("http"):
                self.config["add-repo"] = repo

        self.config["build_type"] = self.args.get("--build", None)
        self.config["enable_eus"] = self.args.get("--enable-eus", False)
        self.config["skip_enabling_rhel_rpms"] = self.args.get("--skip-enabling-rhel-rpms", False)
        self.config["docker-insecure-registry"] = self.args.get("--insecure-registry", False)
        self.config["skip_version_compare"] = self.args.get("skip_version_compare")
        self.config["container_image"] = "%s/%s:%s" % (
            self.docker_registry,
            self.docker_image,
            self.docker_tag,
        )

        report_portal_description = ""
        self.config["ceph_docker_registry"] = self.docker_registry
        report_portal_description += f"docker registry: {self.docker_registry}"
        self.config["ceph_docker_image"] = self.docker_image
        report_portal_description += f"docker image: {self.docker_image}"
        self.config["ceph_docker_image_tag"] = self.docker_tag
        report_portal_description += f"docker registry: {self.docker_registry}"
        self.config["filestore"] = self.args.get("--filestore", False)
        self.config["ec-pool-k-m"] = self.args.get("--use-ec-pool", None)

        if self.args.get("--hotfix-repo"):
            hotfix_repo = self.args.get("--hotfix-repo")
            if hotfix_repo.startswith("http"):
                self.config["hotfix_repo"] = hotfix_repo
        self.config["kernel-repo"] = self.args.get("--kernel-repo", None)

        if self.osp_cred:
            self.config["osp_cred"] = self.osp_cred

        # if Kernel Repo is defined in ENV then set the value in self.config
        if os.environ.get("KERNEL-REPO-URL") is not None:
            self.config["kernel-repo"] = os.environ.get("KERNEL-REPO-URL")

        return self.config
