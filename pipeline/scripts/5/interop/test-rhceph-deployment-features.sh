#! /usr/bin/env bash

# Script to execute install Tier-0 test suite of Red Hat Ceph.
# Maintainers: cephci@redhat.com
# Version: 1.0

random_string=$(cat /dev/urandom | tr -cd 'a-z0-9' | head -c 5)
VM_PREFIX="ci-${random_string}"

# Environment variable overrides
CEPH_PLATFORM=${CEPH_PLATFORM:-"rhel-8"}
PY_CMD=${PY_CMD:-"${HOME}/cephci-venv/bin/python"}
OSP_CRED_FILE=${OSP_CRED_FILE:-}
REPO_FILE=${REPO_FILE:-}
VM_SPEC=${VM_SPEC:-}
RHCS_VERSION=${RHCS_VERSION:-"5.0"}
BUILD_TYPE=${BUILD_TYPE:-"rc"}

echo "Red Hat Ceph Storage 5 Ceph deploy tier-0 test suite execution."

TEST_SUITE="suites/pacific/cephadm/tier-0_cephadm.yaml"
TEST_CONF="conf/pacific/cephadm/sanity-cephadm.yaml"
return_code=0

while [[ $# -gt 0 ]] ; do
    key=$1
    case $key in
        --osp-cred)
            OSP_CRED_FILE=$2
            shift 2 ;;
        --add-repo)
            REPO_FILE=$2
            shift 2 ;;
        --inventory)
            VM_SPEC=$2
            shift 2 ;;
        --platform)
            CEPH_PLATFORM=$2
            shift 2 ;;
        *)
            echo "$1 is unsupported."
            shift 1 ;;
    esac
done

if [ -z "${REPO_FILE}" ] ; then
    echo "Require --add-repo argument."
    exit 1
fi

if [ -z "${VM_SPEC}" ] ; then
    echo "Require --inventory argument."
    exit 1
fi

if [ -z "${OSP_CRED_FILE}" ] ; then
    echo "Require --osp-cred argument."
    exit 1
fi

${PY_CMD} run.py \
    --log-level DEBUG \
    --xunit-results \
    --skip-enabling-rhel-rpms \
    --skip-subscription \
    --rhbuild ${RHCS_VERSION} \
    --platform ${CEPH_PLATFORM} \
    --build ${BUILD_TYPE} \
    --suite ${TEST_SUITE} \
    --global-conf ${TEST_CONF} \
    --instances-name ${VM_PREFIX} \
    --osp-cred ${OSP_CRED_FILE} \
    --inventory ${VM_SPEC} \
    --add-repo ${REPO_FILE}

if [ $? -ne 0 ]; then
    return_code=1
fi

${PY_CMD} run.py --cleanup ${VM_PREFIX} \
    --osp-cred ${OSP_CRED_FILE} \
    --log-level debug

if [ $? -ne 0 ]; then
    echo "SUT cleanup failed for instance having ${VM_PREFIX} prefix."
fi

exit ${return_code}
