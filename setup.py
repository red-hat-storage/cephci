# -*- coding: utf-8 -*-
"""CephCI install and configuration module."""
from setuptools import find_packages, setup

setup(
    name="cephci",
    version="0.2",
    description="Red Hat Ceph CI framework.",
    author="Red Hat Ceph QE",
    author_email="cephqe@redhat.com",
    install_requires=[
        "apache-libcloud>=3.3.0",
    ],
    zip_safe=True,
    packages=find_packages(exclude=["unittests"]),
)
