============================
Multi-Registry Authentication
============================

Overview
========

This feature allows CephCI to authenticate to multiple container registries simultaneously,
solving authentication issues when deploying clusters that pull images from different registries
(e.g., ``registry.redhat.io`` and ``registry.stage.redhat.io``).

Usage
=====

Create a ``podman-auth.json`` file with credentials for multiple registries:

.. code-block:: json

    {
      "auths": {
        "registry.redhat.io": {
          "auth": "base64_encoded_username:password"
        },
        "registry.stage.redhat.io": {
          "auth": "base64_encoded_username:password"
        }
      }
    }

Run CephCI with the auth file:

.. code-block:: bash

    python run.py \
      --custom-config podman-auth-file=/path/to/podman-auth.json \
      --rhbuild 8.1 \
      --suite suites/reef/integrations/test.yaml

The auth file is automatically distributed to ``/etc/ceph/podman-auth.json`` on all cluster nodes.

Example Template
================

See ``examples/podman-auth.json.example`` for a template file.

.. Made with Bob
