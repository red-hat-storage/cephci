# -*- coding: utf-8 -*-
try:
    from setuptools import find_packages, setup
except ImportError:
    from ez_setup import use_setuptools

    use_setuptools()
    from setuptools import find_packages, setup

setup(
    name="cephci",
    version="0.1",
    description="Ceph CI tests that run in jenkins using openstack provider",
    author="Vasu Kulkarni",
    author_email="vasu@redhat.com",
    install_requires=[
        "apache-libcloud>=3.3.0",
        "cryptography",
        "docopt",
        "gevent",
        "greenlet",
        "htmllistparse",
        "ibm-cloud-sdk-core==3.15.0 ; python_version<'3.7'",
        "ibm-cloud-sdk-core ; python_version>='3.7'",
        "ibm-cos-sdk",
        "ibm-cos-sdk-core",
        "ibm-cos-sdk-s3transfer",
        "ibm-vpc>=0.8.0",
        "ibm-cloud-networking-services",
        "jinja_markdown",
        "jinja2",
        "junitparser",
        "paramiko",
        "pyyaml",
        "reportportal-client",
        "requests",
        "softlayer",
    ],
    zip_safe=True,
    include_package_data=True,
    packages=find_packages(exclude=["ez_setup"]),
)
