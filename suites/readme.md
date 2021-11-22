## Red Hat Ceph Storage Test Suites
The test suites that are used for evaluating a Red Hat Ceph Storage are defined
in this folder. They are segregated based on Ceph's upstream release numbers.

## Guidelines
- Segregate the tests based on upstream release name.
- Use lowercase for filenames separated either by `-` or `_`.
- Use `.yaml` as the file extension instead of `.yml`
- Ensure the file follows YAML best practices.
- Use `-` to have a separation within the same category.
- Use `_` to have a separation of category.
- Use soft links instead of copying files between folders.

## Examples
```suite_example
# Generic format
    <qe-tier>_<FG>_<layout>_<suite_name>.yaml

# Singular purpose
    tier-0_deploy_cephadm_greenfield.yaml

# Container
    suites/pacific/rgw/tier-0
    
# Containers - 2
    suites/pacific/rgw/tier-1_multisite/
```
