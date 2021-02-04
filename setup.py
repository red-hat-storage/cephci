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
        "docopt==0.6.2",
        "gevent==1.4.0",
        "greenlet==0.4.16",
        "reportportal-client==3.2.0",
        "requests==2.21.0",
        "paramiko==2.4.2",
        "pyyaml>=4.2b1",
        "jinja2",
        "junitparser==1.4.0",
        "jinja_markdown",
    ],
    zip_safe=True,
    include_package_data=True,
    packages=find_packages(exclude=["ez_setup"]),
)
