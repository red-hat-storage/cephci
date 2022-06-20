#! /usr/bin/env bash

# Script to execute sanity test suite of Red Hat Ceph.
# Maintainers: cephci@redhat.com
# Version: 2.0

random_string=$(cat /dev/urandom | tr -cd 'a-z0-9' | head -c 5)
VM_PREFIX="ci-${random_string}"

# Environment variable overrides
CEPH_PLATFORM=${CEPH_PLATFORM:-}
PY_CMD=${PY_CMD:-"${HOME}/cephci-venv/bin/python"}
OSP_CRED_FILE=${OSP_CRED_FILE:-}
REPO_FILE=${REPO_FILE:-}
VM_SPEC=${VM_SPEC:-}
BUILD_TYPE=${BUILD_TYPE:-"rc"}

echo "Red Hat Ceph Storage sanity test suite execution."

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

if [ -z "${CEPH_PLATFORM}" ] ; then
    echo "Require --platform argument."
    exit 1
fi

declare -A TEST_SUITES

if [ "$CEPH_PLATFORM" == "rhel-9" ]; then
    TEST_SUITES["suites/pacific/interop/test-ceph-sanity.yaml"]="conf/pacific/integrations/6node-all-roles.yaml"
elif [ "$CEPH_PLATFORM" == "rhel-8" ]; then
    TEST_SUITES["suites/pacific/interop/test-ceph-sanity.yaml"]="conf/pacific/integrations/6node-all-roles.yaml"
    TEST_SUITES["suites/nautilus/interop/test-ceph-rpms.yaml"]="conf/nautilus/interop/5-nodes-ceph.yaml"
fi

pids=()
VM_PREFIXES=()
for suite in "${!TEST_SUITES[@]}" ; do
    TEST_SUITE=$suite
    TEST_CONFIG=${TEST_SUITES[$suite]}
    if [[ $TEST_SUITE =~ "nautilus" ]]; then
      RHCS_VERSION="4.2"
    elif [[ $TEST_SUITE =~ "pacific" ]]; then
      RHCS_VERSION="5.1"
    fi

    random_string=$(cat /dev/urandom | tr -cd 'a-z0-9' | head -c 5)
    VM_PREFIX="ci-${random_string}"

    ${PY_CMD} run.py \
      --log-level DEBUG \
      --skip-enabling-rhel-rpms \
      --skip-subscription \
      --rhbuild ${RHCS_VERSION} \
      --platform ${CEPH_PLATFORM} \
      --build ${BUILD_TYPE} \
      --suite ${TEST_SUITE} \
      --global-conf ${TEST_CONFIG} \
      --instances-name ${VM_PREFIX} \
      --osp-cred ${OSP_CRED_FILE} \
      --inventory ${VM_SPEC} \
      --add-repo ${REPO_FILE} \
      --xunit-results &

    pids+=("$!")
    VM_PREFIXES+=("$VM_PREFIX")
done

return_codes=()
while true; do
    for i in "${!pids[@]}"; do
        pid="${pids[$i]}"
        ps --pid "$pid" > /dev/null
        if [ "$?" -ne 0 ]; then
            wait "$pid"
            rc="$?"
            if [ $rc -ne 0 ]; then
                return_codes+=rc
            fi
            unset "pids[$i]"
            num_pids="${#pids[@]}"
            echo "PID $pid is done; return_code = $rc;" \
                 "$num_pids PIDs remaining."
        fi
    done

    if [ "${#pids[@]}" -eq 0 ]; then
        break
    fi
done

if [ "${#return_codes[@]}" -ne 0 ]; then
    return_code=1
fi

c_pids=()
for VM_PREFIX in "${VM_PREFIXES[@]}"; do
    ${PY_CMD} run.py --cleanup ${VM_PREFIX} \
      --osp-cred ${OSP_CRED_FILE} \
      --log-level debug &
    c_pids+=("$!")
done

while true; do
    for i in "${!c_pids[@]}"; do
        c_pid="${c_pids[$i]}"
        ps --pid "$c_pid" > /dev/null
        if [ "$?" -ne 0 ]; then
            wait "$c_pid"
            rc="$?"
            if [ $rc -ne 0 ]; then
                echo "SUT cleanup failed for instance having ${VM_PREFIXES[$i]} prefix."
            fi
            unset "c_pids[$i]"
            num_pids="${#c_pids[@]}"
            echo "PID $c_pid is done; return_code = $rc;" \
                 "$num_pids PIDs remaining."
        fi
    done

    if [ "${#c_pids[@]}" -eq 0 ]; then
        break
    fi
done

exit ${return_code}
