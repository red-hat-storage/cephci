## Red Hat Ceph Storage system layout
The system under test (SUT) configuration along with the storage cluster layout
are defined in this folder. 

| Folders | Description |
| ------- | ----------- |
| `inventory` | Definition of virtual system layout.|
| `jewel` | Cluster layouts used for testing RHCS releases based on Jewel. |
| `luminous` | Cluster layouts used for testing RHCS releases based on Luminous. |
| `nautilus` | Cluster layouts used for testing RHCS releases based on Nautilus. |
| `pacific` | Cluster layouts used for testing RHCS releases based on Pacific. |

Cluster layouts are categorized based on Ceph upstream release name.

## Guidelines
- Use lowercase for filenames separated either by `-` or `_`.
- Use `.yaml` as the file extension instead of `.yml`
- Ensure the file follows YAML best practices.
- Use `-` to have a separation within the same category.
- Use `_` to have a separation of category.
- Use soft links instead of copying files between folders.

```Example
> Filename format.
# <No-of-nodes>_<purpose>.yaml
# 7node_minimal-cluster.yaml

> Deprecated format
# tier-0_rgw_multisite.yaml
```
