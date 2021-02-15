[![Build Status](https://github.com/red-hat-storage/cephci/workflows/tests/badge.svg)](https://github.com/red-hat-storage/cephci/actions)
# Ceph-CI
CEPH-CI is a framework for testing Red Hat Ceph Storage product builds. It
dynamically creates virtual machines and storage volumes within Red Hat's
CentralCI OpenStack enviroment, installs and tests Ceph, and then deallocates
the resources when complete.

## Getting Started

#### Installing
Please use a Python virtual environment to install the necessary dependencies and execute cephci.

1. Setup a Python virtualenv (CEPH-CI requires Python 3.6 or newer).
    * `python3 -m venv <path/to/venv>`
    * `source <path/to/venv>/bin/activate`
2. Install the latest version of Pip into your virtualenv:
    * `pip install --upgrade pip`
3. Install cephci's dependencies:
    * `pip install -r requirements.txt`

#### Initial Setup
Configure your cephci.yaml file:

This file is used to allow configuration around a number of things within cephci.
The template can be found at the top level of the repository, `cephci.yaml.template`.
The required keys are in the template. Values are placeholders and should be replaced by legitimate values.
Values for report portal or polarion are only required if you plan on posting to that particular service.

Move a copy of the template to your user directory and edit it from there with the proper values.
```
cp cephci.yaml.template ~/.cephci.yaml
```

## Tests
#### CentralCI Authentication
CentralCI auth files are kept in the `osp` directory.

The `osp-cred-ci-2.yaml` file has OpenStack credentials details to create/destroy resources.
For local cephci runs, you will want to replace the username/password with
your own OpenStack credentials.

#### Cluster Configuration
Cluster configuration files are kept in a directory under `conf` for each ceph version.

| Ceph upstream codename | RH Ceph Storage Release | Location        |
| ---------------------- | ----------------------- | --------------- |
| "Jewel"                | RH Ceph Storage 2       | `conf/jewel`    |
| "Luminous              | RH Ceph Storage 3       | `conf/luminous` |
| "Nautilus              | RH Ceph Storage 4       | `conf/nautilus` |
| "Pacific"              | RH Ceph Storage 5       | `conf/pacific`  |

The image-name inside globals: define what image is used to clone ceph-nodes(
mon, osd, mds etc), The role maps to ceph role that the node will take
and osd generally attach 3 additional volumes with disk-size as specified in
config.

#### Inventory Files
Inventory files are kept under `conf/inventory`,
and are used to specify which operating system to be used for the cluster resources.

#### Test Suites
All test suite configurations are found inside the `suites` directory.

There are various suites that are mapped to versions of Ceph under test

```
suites/jewel/ansible/sanity_ceph_ansible will be valid for 2.0 builds
suites/luminous/ansible/sanity_ceph_ansible will be valid for 3.0 builds
```
The tests inside the suites are described in yaml format

```
tests:
   - test:
      name: ceph deploy
      module: test_ceph_deploy.py
      config:
        base_url: 'http://download-node-02.eng.bos.redhat.com/rcm-guest/ceph-drops/auto/ceph-1.3-rhel-7-compose/RHCEPH-1.3-RHEL-7-20161010.t.0/'
        installer_url: 
      desc: test cluster setup using ceph-deploy
      destroy-cluster: False
      abort-on-fail: True
      
   - test:
      name: rados workunit
      module: test_workunit.py
      config:
            test_name: rados/test_python.sh
            branch: hammer
      desc: Test rados python api
```
The above snippet describes two tests and the module is the name of the python
script that is executed to verify the test, every module can take a config
dict that is passed to it from the run wrapper, The run wrapper executes
the tests serially found in the suites. The test scripts are location in
the `tests` folder.

## Usage
`run.py` is the main script for ceph-ci. You can view the full usage details by passing in the `--help` argument.
```
python run.py --help
```
#### Required Arguments
There are a few arguments that are required for cephci execution:

* `--rhbuild <build_version>`
* `--osp-cred <cred_file>`
* `--global-conf <conf_file>`
* `--inventory <inventory_file>`
* `--suite <suite_file>`

#### Useful Arguments
Some non-required arguments that we end up using a lot:
* `--log level <level>` - set the log level that is output to stdout.

#### Examples
Ceph ansible install suite:
```
python run.py --rhbuild 3.3 --global-conf conf/luminous/ansible/sanity-ansible-lvm.yaml --osp-cred osp/osp-cred-ci-2.yaml
--inventory conf/inventory/rhel-7.8-server-x86_64.yaml --suite suites/luminous/ansible/sanity_ceph_ansible_lvm.yaml
--log-level info
```
Upgrade suite:
```
python run.py --rhbuild 3.2 --global-conf conf/luminous/upgrades/upgrade.yaml --osp-cred osp/osp-cred-ci-2.yaml
--inventory conf/inventory/rhel-7.8-server-x86_64-released.yaml --suite suites/luminous/upgrades/upgrades.yaml
--log-level info
```
Containerized upgrade suite:
```
python run.py --rhbuild 3.2 --global-conf conf/luminous/upgrades/upgrade.yaml --osp-cred osp/osp-cred-ci-2.yaml
--inventory conf/inventory/rhel-7.8-server-x86_64-released.yaml --suite suites/luminous/upgrades/upgrades_containerized.yaml
--log-level info --ignore-latest-container --insecure-registry --skip-version-compare
```

#### Manual cluster cleanup
Ceph-CI also has the ability to manually clean up cluster nodes if anything was left behind during a test run.
All you need to provide is your osp credentials and the instances name for the cluster. Don't use subset naming for custom instances name.eg: --instances-name vp and --instances-name vpoliset  at same time.

For example, this command will delete all volumes and nodes that have the substring `ceph-kdreyer` in their names:
```
python run.py --osp-cred <cred_file> --log-level info --cleanup ceph-kdreyer
```

## Results
In order to post results properly or receive results emails you must first configure your `~/.cephci.yaml` file.
Please see the [Initial Setup](#initial-setup) section of the readme if you haven't done that.

#### Polarion
Results are posted to polarion if the `--post-results` argument is passed to `run.py`.
When this argument is used, any tests that have a `polarion-id` configured in the suite
will have it's result posted to polarion.

#### Report Portal
Results are posted to report portal if the `--report-portal` argument is passed to `run.py`.

#### Email
A result email is automatically sent to the address configured in your `~/.cephci.yaml` file.
In addition to personally configured emails, if the `--post-results` or `--report-portal` arguments are
passed to `run.py` an email will also be sent to `cephci@redhat.com`.

## Development Guidelines
An attempt is made to follow a set of style guidelines for clarity. As we grow
and have more contributors, it becomes essential for us to follow some standard
practices. We use

- PEP8
- black
- isort

### black
```black_info
# Black is installed as part of cephci requirements. One can also install it
$ python -m pip install black

# formatting code with black
$ black <filename>

where `filename`
- a relative or absolute path of the file
- a relative or absolute path of a directory # for running for set of files

# for the project
$ black .
```

### isort
```isort_info
# isort is installed as part of the cephci requirements. One can also install the
# package using the below command
$ python -m pip install isort

# formatting code with isort
$ isort <filename>

where `filename`
- a relative or absolute path of the file
- a relative or absolute path of a directory

# Example
$ isort mita/openstack.py
```
