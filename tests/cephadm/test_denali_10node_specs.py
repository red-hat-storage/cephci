"""Draft CephCI adapter: preserve native service specs and apply desired-state YAML.

Fresh dedicated test clusters only. No source cluster credentials or disk zapping.
"""
import base64
import copy
import json
import shlex
import time
import uuid

import yaml
from ceph.ceph_admin import CephAdmin
from ceph.utils import get_node_by_id
from utility.log import Log

LOG = Log(__name__)


def run(ceph_cluster, **kw):
    cfg = kw['config']
    admin = CephAdmin(cluster=ceph_cluster, **cfg)
    node = lambda name: get_node_by_id(ceph_cluster, name)
    hostname = lambda name: node(name).hostname

    def ceph(*args, mount=None):
        argv = ['ceph'] + [str(a).lower() if isinstance(a, bool) else str(a) for a in args]
        cmd = ' '.join(shlex.quote(a) for a in argv)
        options = {'mount': f'{mount}:{mount}'} if mount else {}
        out, _ = admin.shell(args=[cmd], base_cmd_args=options)
        return out

    def read(*args):
        return json.loads(ceph(*args, '-f', 'json'))

    def wait(label, predicate, seconds=1800):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(15)
        raise RuntimeError(f'Timeout waiting for {label}')

    def apply_file(content, *args):
        path = '/tmp/denali-' + uuid.uuid4().hex + '.yaml'
        try:
            encoded = base64.b64encode(content.encode()).decode()
            admin.installer.exec_command(sudo=True, cmd=(
                "umask 077; printf %s " + shlex.quote(encoded)
                + " | base64 -d > " + shlex.quote(path)))
            return ceph(*args, '-i', path, mount=path)
        finally:
            admin.installer.exec_command(sudo=True, cmd='rm -f ' + shlex.quote(path))

    def apply_specs(specs):
        docs = copy.deepcopy(specs)
        for spec in docs:
            placement = spec.get('placement', {})
            if 'nodes' in placement:
                placement['hosts'] = [hostname(n) for n in placement.pop('nodes')]
        apply_file(yaml.safe_dump_all(docs, explicit_start=True, sort_keys=False), 'orch', 'apply')

    def assert_hosts(expected):
        actual = {h['hostname']: h for h in read('orch', 'host', 'ls')}
        assert set(actual) == {hostname(n) for n in expected}, 'Unexpected/missing hosts'
        for n, spec in expected.items():
            h = actual[hostname(n)]
            assert set(h['labels']) == set(spec['labels']), f'Label mismatch: {n}: {h["labels"]}'
        # 'orch host ls' does not report the host location; it is applied as a CRUSH bucket.
        # Hosts without a location (no OSDs) are not placed in the CRUSH tree.
        tree = read('osd', 'tree')['nodes']
        crush_hosts = {h['name']: h['id'] for h in tree if h['type'] == 'host'}
        zones = {h['name']: set(h['children']) for h in tree if h['type'] == 'zone'}
        for n, spec in expected.items():
            zone = spec['location'].get('zone')
            if not zone:
                continue
            assert crush_hosts.get(hostname(n)) in zones.get(zone, set()), f'CRUSH location mismatch: {n} not under {zone}'

    def assert_osds(expected):
        tree = read('osd', 'tree')['nodes']
        hosts = {h['name']: h for h in tree if h['type'] == 'host'}
        zones = {h['name']: set(h['children']) for h in tree if h['type'] == 'zone'}
        devices = {h['id']: h for h in tree if h['type'] == 'osd'}
        for n, spec in expected.items():
            if not spec['osds']:
                continue
            h = hosts.get(hostname(n), {})
            if len(h.get('children', [])) != spec['osds'] or h.get('id') not in zones.get(spec['location']['zone'], set()):
                return False
            if not all(devices[i]['status'] == 'up' and devices[i].get('reweight', 0) > 0 and devices[i].get('device_class') == 'ssd' for i in h['children']):
                return False
        return len(devices) == sum(h['osds'] for h in expected.values())

    def services_ready(expected):
        actual = {s['service_name']: s['status'].get('running', 0) for s in read('orch', 'ls')}
        return all(actual.get(k) == v for k, v in expected.items())

    def fsmap(fs):
        return next(x['mdsmap'] for x in read('fs', 'dump')['filesystems'] if x['mdsmap']['fs_name'] == fs)

    def assert_crush_rule(rule):
        actual = next(r for r in read('osd', 'crush', 'rule', 'dump')
                      if r['rule_name'] == rule['name'])
        steps = actual['steps']
        assert actual['type'] == 1 and len(steps) == 3, 'Unexpected CRUSH rule type/length'
        assert steps[0]['op'] == 'take' and steps[0]['item_name'] == rule['root']
        assert steps[1]['op'] == 'chooseleaf_firstn' and steps[1]['num'] == 0
        assert steps[1]['type'] == rule['failure_domain'] and steps[2]['op'] == 'emit'

    def assert_zone_services(expected):
        daemons = read('orch', 'ps')
        for service in ('mon', 'mgr', *(f'nfs.nfsc{i}' for i in range(1, 7))):
            label = service.split('.')[-1]
            desired = {hostname(n) for n, h in expected.items()
                       if any(f'{label}_z{z}' in h['labels'] for z in (1, 2, 3))}
            running = [d['hostname'] for d in daemons
                       if d['service_name'] == service and d.get('status') == 1]
            assert len(desired) == len(running) == 3 and set(running) == desired, service

    def pg_zones_ready(expected, pools):
        tree = read('osd', 'tree')['nodes']
        hosts = {h['name']: h for h in tree if h['type'] == 'host'}
        zones = {h['name']: set(h['children']) for h in tree if h['type'] == 'zone'}
        osd_zone = {}
        for n, h in expected.items():
            if not h['osds']:
                continue
            host = hosts[hostname(n)]
            zone = h['location']['zone']
            assert host['id'] in zones[zone], f'Host {n} outside {zone}'
            for osd in host['children']:
                osd_zone[osd] = zone
        actual_pools = {p['pool_name']: p for p in read('osd', 'pool', 'ls', 'detail')}
        rules = {r['rule_id']: r['rule_name'] for r in read('osd', 'crush', 'rule', 'dump')}
        pool_ids = set()
        for p in pools:
            actual = actual_pools[p['name']]
            assert rules[actual['crush_rule']] == p['crush_rule']
            assert actual['size'] == 3 and actual['min_size'] == p['min_size']
            pool_ids.add(actual['pool_id'])
        dump = read('pg', 'dump', 'pgs')
        pgs = dump.get('pg_stats', dump.get('pg_map', {}).get('pg_stats', []))
        seen = set()
        for pg in pgs:
            pid = int(pg['pgid'].split('.')[0])
            if pid not in pool_ids:
                continue
            seen.add(pid)
            if not {'active', 'clean'} <= set(pg['state'].split('+')):
                return False
            for key in ('up', 'acting'):
                if len(pg[key]) != 3 or {osd_zone.get(i) for i in pg[key]} != {'zone1', 'zone2', 'zone3'}:
                    return False
        return seen == pool_ids

    action = cfg['action']
    if action == 'preflight':
        if not cfg.get('deployment_reviewed'):
            raise RuntimeError('Draft: verify IBM Cloud inventory, profile, network, image and isolated fresh data disks, then set deployment_reviewed: true')
        for n, spec in cfg['expected_hosts'].items():
            if spec['osds'] and cfg.get('require_tpm2'):
                node(n).exec_command(sudo=True, cmd='test -c /dev/tpmrm0')
        for n in cfg.get('rdma_nodes', []):
            node(n).exec_command(sudo=True, cmd="rdma link show | grep -w ACTIVE")
    elif action == 'control':
        def hosts_ready():
            try:
                assert_hosts(cfg['expected_hosts'])
                return True
            except AssertionError as e:
                LOG.info(f'Hosts not ready: {e}')
                return False
        wait('host registration and labels', hosts_ready, 1800)
        apply_specs(cfg['specs'])
        wait('MON/MGR', lambda: services_ready({'mon': 3, 'mgr': 3}))
    elif action == 'osd':
        expected = cfg['expected_hosts']
        assert_hosts(expected)
        apply_specs(cfg['specs'])
        wait('all OSDs up/in in intended zones', lambda: assert_osds(expected), 3600)
    elif action == 'crush':
        rule = cfg['crush_rule']
        existing = read('osd', 'crush', 'rule', 'ls')
        if rule['name'] not in existing:
            ceph('osd', 'crush', 'rule', 'create-replicated', rule['name'], rule['root'], rule['failure_domain'])
        assert_crush_rule(rule)
    elif action == 'settings':
        for section, options in cfg['settings'].items():
            for key, value in options.items():
                ceph('config', 'set', section, key, value)
    elif action == 'pools':
        for pool in cfg['pools']:
            name = pool['name']
            if name not in read('osd', 'pool', 'ls'):
                # The mon rejects pool names starting with '.' unless explicitly confirmed (as the nfs mgr module does).
                confirm = ['--yes-i-really-mean-it'] if name.startswith('.') else []
                ceph('osd', 'pool', 'create', name, pool['pg_num'], pool['pg_num'], 'replicated', pool['crush_rule'], *confirm)
            for key in ('crush_rule', 'size', 'min_size', 'pg_autoscale_mode'):
                ceph('osd', 'pool', 'set', name, key, pool[key])
            if 'bulk' in pool['flags'].split(','):
                ceph('osd', 'pool', 'set', name, 'bulk', True)
            for key, value in pool['options'].items():
                ceph('osd', 'pool', 'set', name, key, value)
            # Autoscaling may change observed PGs; do not force a pool PG decrease.
            if name in ('.mgr', '.nfs'):
                for app in pool['application_metadata']:
                    ceph('osd', 'pool', 'application', 'enable', name, app)
    elif action == 'pg_zones':
        wait('clean PGs with one up and acting replica per zone', lambda: pg_zones_ready(cfg['expected_hosts'], cfg['pools']), 3600)
    elif action == 'filesystem':
        fs = cfg['filesystem']
        if fs['name'] not in [f['name'] for f in read('fs', 'ls')]:
            ceph('fs', 'new', fs['name'], fs['metadata_pool'], fs['data_pools'][0])
    elif action == 'mds':
        fs = cfg['filesystem']
        apply_specs(cfg['specs'])
        wait('MDS daemons', lambda: services_ready({'mds.' + fs['name']: cfg['expected_mds']}), 3600)
        for key in ('max_mds', 'allow_standby_replay', 'standby_count_wanted', 'balance_automate', 'max_file_size'):
            ceph('fs', 'set', fs['name'], key, fs[key])
        def ranks_ready():
            states = [i['state'] for i in fsmap(fs['name'])['info'].values()]
            return states.count('up:active') == fs['max_mds'] and states.count('up:standby-replay') == fs['max_mds']
        wait('active and standby-replay ranks', ranks_ready, 3600)
    elif action == 'services':
        apply_specs(cfg['specs'])
    elif action == 'exports':
        fs = cfg['filesystem']
        wait('NFS services', lambda: services_ready({f'nfs.nfsc{i}': 3 for i in range(1, 7)}), 1800)
        for subvol in cfg['subvolumes']:
            ceph('fs', 'subvolume', 'create', fs, subvol['name'])
        for export in cfg['exports']:
            doc = copy.deepcopy(export)
            name = doc.pop('subvolume')
            doc['path'] = ceph('fs', 'subvolume', 'getpath', fs, name).strip()
            assert doc['path'].startswith('/volumes/'), 'Unexpected subvolume path'
            # Credentials are generated by the target NFS manager.
            apply_file(json.dumps(doc), 'nfs', 'export', 'apply', doc['cluster_id'])
    elif action == 'client':
        for client in cfg.get('clients') or [cfg['client']]:
            node(client).exec_command(sudo=True, cmd='dnf -y install nfs-utils', timeout=600)
    elif action == 'validate':
        assert_hosts(cfg['expected_hosts'])
        assert assert_osds(cfg['expected_hosts']), 'OSD placement/count mismatch'
        wait('all expected services', lambda: services_ready(cfg['expected_services']), 3600)
        assert_zone_services(cfg['expected_hosts'])
        states = [i['state'] for i in fsmap(cfg['filesystem'])['info'].values()]
        assert states.count('up:active') == cfg['max_mds']
        assert states.count('up:standby-replay') == cfg['max_mds']
        assert_crush_rule(cfg['crush_rule'])
        wait('final PG zone distribution', lambda: pg_zones_ready(cfg['expected_hosts'], cfg['pools']), 3600)
        for i in range(1, 7):
            cid = f'nfsc{i}'
            actual = {e['pseudo']: e for e in read('nfs', 'export', 'ls', cid, '--detailed')}
            expected = {e['pseudo']: e for e in cfg['exports'] if e['cluster_id'] == cid}
            assert set(actual) == set(expected), f'NFS export set mismatch: {cid}'
            for pseudo, e in expected.items():
                for key in ('access_type', 'protocols', 'security_label', 'squash', 'transports', 'clients'):
                    assert actual[pseudo][key] == e[key], f'NFS export {pseudo}: {key}'
                assert actual[pseudo]['path'] == ceph('fs', 'subvolume', 'getpath', cfg['filesystem'], e['subvolume']).strip()
        pools = {p['pool_name']: p for p in read('osd', 'pool', 'ls', 'detail')}
        rules = {r['rule_id']: r['rule_name'] for r in read('osd', 'crush', 'rule', 'dump')}
        assert set(pools) == {p['name'] for p in cfg['pools']}, 'Unexpected pools'
        for p in cfg['pools']:
            actual = pools[p['name']]
            assert rules[actual['crush_rule']] == p['crush_rule']
            assert actual['size'] == p['size'] and actual['min_size'] == p['min_size']
            LOG.info(f"{p['name']}: desired PGs {p['pg_num']}; actual {actual['pg_num']} (autoscaler off for initial verification)")
        ceph('status')
    else:
        raise ValueError(f'Unknown action {action}')
    return 0
