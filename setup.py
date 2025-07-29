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
    description="A test framework to evaluate Ceph.",
    author="Ceph QE Team",
    author_email="cephci@redhat.com",
    install_requires=[
        "apache-libcloud==3.8.0",
        "cryptography==45.0.4",
        "docopt==0.6.2",
        "htmllistparse==0.6.1",
        "ibm-cloud-sdk-core==3.24.2",
        "ibm-vpc==0.29.1",
        "ibm-cloud-networking-services==0.25.0",
        "jinja_markdown==1.210911",
        "jinja2==3.1.6",
        "junitparser==4.0.2",
        "looseversion==1.3.0",
        "paramiko==3.5.1",
        "plotly==6.1.2",
        "pyyaml==6.0.2",
        "requests==2.32.4",
        "python-ipmi==0.5.7",
    ],
    zip_safe=True,
    include_package_data=True,
    packages=find_packages(exclude=["ez_setup"]),
)
