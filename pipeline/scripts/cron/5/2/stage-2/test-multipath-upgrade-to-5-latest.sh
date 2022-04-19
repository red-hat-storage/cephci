#! /bin/sh
echo "Test multiple path upgrade to 5x latest"

random_string=$(cat /dev/urandom | tr -cd 'a-z0-9' | head -c 5)
instance_name="ci-${random_string}"
platform="rhel-8"
rhbuild="5.2"
test_suite="suites/pacific/upgrades/tier-2_upgrade_test-multi-path-upgrade-to-5-latest.yaml"
test_conf="conf/pacific/upgrades/tier-1_upgrade_cephadm.yaml"
test_inventory="conf/inventory/rhel-8-latest.yaml"
return_code=0

# Process the CLI arguments for IBM-C environment
CLI_ARGS=$@
cloud="ibmc"
if [ -z "${CLI_ARGS##*$cloud*}" ] ; then
    test_inventory="conf/inventory/ibm-vpc-rhel-8-latest.yaml"
else
    CLI_ARGS="$CLI_ARGS --post-results --report-portal"
fi

$WORKSPACE/.venv/bin/python run.py \
    --osp-cred $HOME/osp-cred-ci-2.yaml \
    --rhbuild $rhbuild \
    --platform $platform \
    --instances-name $instance_name \
    --global-conf $test_conf \
    --suite $test_suite \
    --inventory $test_inventory \
    --log-level DEBUG \
    $CLI_ARGS

if [ $? -ne 0 ]; then
    return_code=1
fi

CLEANUP_ARGS="--log-level debug --osp-cred $HOME/osp-cred-ci-2.yaml"
if [ -z "${CLI_ARGS##*$cloud*}" ] ; then
    CLEANUP_ARGS="$CLEANUP_ARGS --cloud ibmc"
fi

$WORKSPACE/.venv/bin/python run.py --cleanup $instance_name $CLEANUP_ARGS

if [ $? -ne 0 ]; then
    echo "cleanup instance failed for instance $instance_name"
fi

exit ${return_code}
