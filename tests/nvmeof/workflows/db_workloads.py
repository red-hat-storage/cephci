"""
Customer database workloads on NVMeoF block devices.

Pattern:
  NVMe namespace -> mkfs/mount -> podman DB with data dir on mount -> native bench

Feasible in cephci (no app licenses):
  postgresql, mysql, mariadb, mongodb, redis, cassandra

Not automated here (license / heavy external deps):
  oracle, mssql  — logged as skipped unless explicitly enabled (mssql needs EULA)
"""

import time

from utility.log import Log

LOG = Log(__name__)

# Public images — avoid registry.redhat.io auth requirements on bare clients
DEFAULT_IMAGES = {
    "postgresql": "docker.io/library/postgres:16",
    "mysql": "docker.io/library/mysql:8.4",
    "mariadb": "docker.io/library/mariadb:11.4",
    "mongodb": "docker.io/library/mongo:7",
    "redis": "docker.io/library/redis:7",
    "cassandra": "docker.io/library/cassandra:4.1",
    "mssql": "mcr.microsoft.com/mssql/server:2022-latest",
}

UNSUPPORTED = {
    "oracle": "Oracle Database requires OTN license / proprietary container; skip in cephci",
}


def _run(node, cmd, timeout=600, check_ec=True):
    out, err = node.exec_command(sudo=True, cmd=cmd, timeout=timeout, check_ec=check_ec)
    return (out or "").strip(), (err or "").strip()


def ensure_podman(node):
    out, _ = _run(node, "command -v podman || true", check_ec=False)
    if out:
        return
    LOG.info("Installing podman on %s", node.hostname)
    _run(node, "yum install -y podman", timeout=900)


def mkfs_and_mount(node, device, mount_point, fstype="xfs"):
    """Format NVMe path and mount for DB datadir."""
    _run(node, f"mkdir -p {mount_point}")
    # Already mounted?
    mounts, _ = _run(node, "findmnt -n -o TARGET || true", check_ec=False)
    if mount_point in mounts.splitlines():
        LOG.info("%s already mounted; reusing", mount_point)
        return

    if fstype == "xfs":
        _run(node, f"mkfs.xfs -f {device}", timeout=300)
    else:
        _run(node, f"mkfs.ext4 -F {device}", timeout=300)
    _run(node, f"mount {device} {mount_point}")
    LOG.info("Mounted %s -> %s (%s)", device, mount_point, fstype)


def umount_quiet(node, mount_point):
    _run(node, f"umount {mount_point} || true", check_ec=False)


def podman_rm(node, name):
    _run(node, f"podman rm -f {name} || true", check_ec=False)


def wait_container_running(node, name, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        out, _ = _run(
            node,
            f"podman inspect -f '{{{{.State.Running}}}}' {name} 2>/dev/null || echo false",
            check_ec=False,
        )
        if out.strip().lower() == "true":
            return True
        time.sleep(3)
    return False


def wait_cmd_ok(node, cmd, timeout=180, interval=5):
    end = time.time() + timeout
    while time.time() < end:
        try:
            node.exec_command(sudo=True, cmd=cmd, timeout=60)
            return True
        except Exception:
            time.sleep(interval)
    LOG.error("Timed out waiting for: %s", cmd)
    return False


def _image(engine, cfg):
    return cfg.get("image") or DEFAULT_IMAGES[engine]


# ---------------------------------------------------------------------------
# Engines
# ---------------------------------------------------------------------------


def run_postgresql(node, data_dir, cfg):
    name = cfg.get("container_name", "nvmeof-pg")
    image = _image("postgresql", cfg)
    db = cfg.get("db_name", "nvmeofdb")
    user = cfg.get("user", "pguser")
    password = cfg.get("password", "pgpassword")
    port = int(cfg.get("port", 5432))
    scale = int(cfg.get("scale", 20))
    clients = int(cfg.get("clients", 8))
    jobs = int(cfg.get("jobs", 4))
    duration = int(cfg.get("duration", 60))

    podman_rm(node, name)
    _run(node, f"mkdir -p {data_dir} && chmod 777 {data_dir}")
    _run(
        node,
        (
            f"podman run -d --name {name} "
            f"-e POSTGRES_USER={user} "
            f"-e POSTGRES_PASSWORD={password} "
            f"-e POSTGRES_DB={db} "
            f"-v {data_dir}:/var/lib/postgresql/data:Z "
            f"-p {port}:5432 "
            f"{image}"
        ),
        timeout=300,
    )
    if not wait_container_running(node, name):
        raise RuntimeError(f"PostgreSQL container {name} failed to start")
    ready = f"podman exec {name} pg_isready -U {user} -d {db}"
    if not wait_cmd_ok(node, ready, timeout=180):
        raise RuntimeError("PostgreSQL not ready")

    LOG.info("pgbench init scale=%s on %s", scale, data_dir)
    _run(
        node,
        (f"podman exec {name} pgbench -i -s {scale} -U {user} -d {db}"),
        timeout=3600,
    )
    LOG.info("pgbench run clients=%s jobs=%s duration=%ss", clients, jobs, duration)
    _run(
        node,
        (
            f"podman exec {name} pgbench -c {clients} -j {jobs} -T {duration} "
            f"-U {user} -d {db}"
        ),
        timeout=duration + 600,
    )
    return {"engine": "postgresql", "container": name, "data_dir": data_dir}


def run_mysql_family(node, data_dir, cfg, engine="mysql"):
    """MySQL or MariaDB via sysbench-like mysqlslap / built-in mysql client."""
    name = cfg.get("container_name", f"nvmeof-{engine}")
    image = _image(engine, cfg)
    db = cfg.get("db_name", "nvmeofdb")
    root_pw = cfg.get("password", "RootPassw0rd!")
    port = int(cfg.get("port", 3306 if engine == "mysql" else 3307))
    duration = int(cfg.get("duration", 60))
    concurrency = int(cfg.get("clients", 8))
    datadir_ctr = "/var/lib/mysql"

    podman_rm(node, name)
    _run(node, f"mkdir -p {data_dir} && chmod 777 {data_dir}")
    env = f"-e MYSQL_ROOT_PASSWORD={root_pw} -e MYSQL_DATABASE={db}"
    if engine == "mariadb":
        env = f"-e MARIADB_ROOT_PASSWORD={root_pw} -e MARIADB_DATABASE={db}"

    _run(
        node,
        (
            f"podman run -d --name {name} {env} "
            f"-v {data_dir}:{datadir_ctr}:Z "
            f"-p {port}:3306 "
            f"{image}"
        ),
        timeout=300,
    )
    if not wait_container_running(node, name):
        raise RuntimeError(f"{engine} container {name} failed to start")

    ping = f"podman exec {name} mysqladmin ping -uroot -p{root_pw} --silent"
    if not wait_cmd_ok(node, ping, timeout=240):
        raise RuntimeError(f"{engine} not ready")

    # Schema + bulk load via SQL
    _run(
        node,
        (
            f"podman exec {name} mysql -uroot -p{root_pw} {db} -e "
            f'"CREATE TABLE IF NOT EXISTS kv (id INT PRIMARY KEY, val VARCHAR(128));"'
        ),
        timeout=120,
    )
    rows = int(cfg.get("rows", 20000))
    _run(
        node,
        (
            f"podman exec {name} bash -lc "
            f"'mysql -uroot -p{root_pw} {db} -e "
            f'"INSERT INTO kv (id, val) VALUES (1, \\"v1\\") '
            f'ON DUPLICATE KEY UPDATE val=VALUES(val);" ; '
            f"for i in $(seq 2 {rows}); do "
            f'echo "INSERT INTO kv (id,val) VALUES ($i, \\"v$i\\") '
            f'ON DUPLICATE KEY UPDATE val=VALUES(val);"; '
            f"done | mysql -uroot -p{root_pw} {db}'"
        ),
        timeout=1800,
    )

    # mysqlslap OLTP-ish mixed statements
    LOG.info("%s mysqlslap concurrency=%s duration~%ss", engine, concurrency, duration)
    _run(
        node,
        (
            f"podman exec {name} mysqlslap -uroot -p{root_pw} "
            f"--concurrency={concurrency} --iterations=5 "
            f"--number-of-queries={max(1000, duration * 50)} "
            f"--create-schema={db} "
            f'--query="SELECT val FROM kv WHERE id=FLOOR(1+RAND()*{rows}); '
            f"UPDATE kv SET val=CONCAT('u', id) WHERE id=FLOOR(1+RAND()*{rows});\""
        ),
        timeout=duration + 900,
    )
    return {"engine": engine, "container": name, "data_dir": data_dir}


def run_mongodb(node, data_dir, cfg):
    name = cfg.get("container_name", "nvmeof-mongo")
    image = _image("mongodb", cfg)
    port = int(cfg.get("port", 27017))
    duration = int(cfg.get("duration", 60))
    docs = int(cfg.get("docs", 50000))

    podman_rm(node, name)
    _run(node, f"mkdir -p {data_dir} && chmod 777 {data_dir}")
    _run(
        node,
        (
            f"podman run -d --name {name} "
            f"-v {data_dir}:/data/db:Z "
            f"-p {port}:27017 "
            f"{image}"
        ),
        timeout=300,
    )
    if not wait_container_running(node, name):
        raise RuntimeError(f"MongoDB container {name} failed to start")
    ready = f"podman exec {name} mongosh --quiet --eval 'db.runCommand({{ ping: 1 }})'"
    if not wait_cmd_ok(node, ready, timeout=180):
        raise RuntimeError("MongoDB not ready")

    LOG.info("MongoDB load %s docs then timed updates (%ss)", docs, duration)
    _run(
        node,
        (
            f"podman exec {name} mongosh --quiet --eval "
            f'\'db = db.getSiblingDB("nvmeof"); '
            f"db.dropDatabase(); "
            f"const bulk=[]; for (let i=0;i<{docs};i++) "
            f'bulk.push({{i:i, v:"x"+i, t:new Date()}}); '
            f"while(bulk.length) {{ db.coll.insertMany(bulk.splice(0,1000)); }} "
            f"db.coll.createIndex({{i:1}}); "
            f"const end=Date.now()+{duration*1000}; let n=0; "
            f"while(Date.now()<end) {{ "
            f"db.coll.updateOne({{i: Math.floor(Math.random()*{docs})}}, "
            f'{{$set:{{v:"u"+(n++)}}}}); '
            f"db.coll.findOne({{i: Math.floor(Math.random()*{docs})}}); "
            f"}} "
            f'print("ops="+n);\''
        ),
        timeout=duration + 900,
    )
    return {"engine": "mongodb", "container": name, "data_dir": data_dir}


def run_redis(node, data_dir, cfg):
    name = cfg.get("container_name", "nvmeof-redis")
    image = _image("redis", cfg)
    port = int(cfg.get("port", 6379))
    duration = int(cfg.get("duration", 60))
    clients = int(cfg.get("clients", 50))
    requests = int(cfg.get("requests", max(100000, duration * 2000)))

    podman_rm(node, name)
    _run(node, f"mkdir -p {data_dir} && chmod 777 {data_dir}")
    # AOF persistence on NVMe mount
    _run(
        node,
        (
            f"podman run -d --name {name} "
            f"-v {data_dir}:/data:Z "
            f"-p {port}:6379 "
            f"{image} redis-server --appendonly yes --dir /data"
        ),
        timeout=300,
    )
    if not wait_container_running(node, name):
        raise RuntimeError(f"Redis container {name} failed to start")
    if not wait_cmd_ok(node, f"podman exec {name} redis-cli ping", timeout=120):
        raise RuntimeError("Redis not ready")

    LOG.info("redis-benchmark clients=%s requests=%s", clients, requests)
    _run(
        node,
        (
            f"podman exec {name} redis-benchmark -c {clients} -n {requests} "
            f"-t set,get,lpush,lpop,hset -q"
        ),
        timeout=duration + 900,
    )
    # Force AOF rewrite to stress persistence path
    _run(node, f"podman exec {name} redis-cli BGREWRITEAOF", check_ec=False)
    time.sleep(5)
    return {"engine": "redis", "container": name, "data_dir": data_dir}


def run_cassandra(node, data_dir, cfg):
    name = cfg.get("container_name", "nvmeof-cassandra")
    image = _image("cassandra", cfg)
    port = int(cfg.get("port", 9042))
    ops = int(cfg.get("ops", 50000))

    podman_rm(node, name)
    _run(node, f"mkdir -p {data_dir} && chmod 777 {data_dir}")
    _run(
        node,
        (
            f"podman run -d --name {name} "
            f"-e MAX_HEAP_SIZE=512M -e HEAP_NEWSIZE=128M "
            f"-v {data_dir}:/var/lib/cassandra:Z "
            f"-p {port}:9042 "
            f"{image}"
        ),
        timeout=300,
    )
    if not wait_container_running(node, name):
        raise RuntimeError(f"Cassandra container {name} failed to start")

    # Cassandra native start is slow
    ready = f'podman exec {name} cqlsh -e "DESCRIBE KEYSPACES"'
    if not wait_cmd_ok(node, ready, timeout=300, interval=10):
        raise RuntimeError("Cassandra not ready")

    LOG.info("Cassandra cql write/read stress ops~%s", ops)
    _run(
        node,
        (
            f"podman exec {name} cqlsh -e "
            f'"CREATE KEYSPACE IF NOT EXISTS nvmeof WITH replication = '
            f"{{'class':'SimpleStrategy','replication_factor':1}}; "
            f'CREATE TABLE IF NOT EXISTS nvmeof.kv (id int PRIMARY KEY, v text);"'
        ),
        timeout=120,
    )
    # Batch inserts via python-less bash loop inside container
    _run(
        node,
        (
            f"podman exec {name} bash -lc "
            f"'for i in $(seq 1 {min(ops, 20000)}); do "
            f"echo \"INSERT INTO nvmeof.kv (id,v) VALUES ($i, '\\''v$i'\\'');\" ; "
            f"done | cqlsh'"
        ),
        timeout=3600,
    )
    _run(
        node,
        (
            f"podman exec {name} cqlsh -e "
            f'"SELECT COUNT(*) FROM nvmeof.kv; '
            f'SELECT * FROM nvmeof.kv WHERE id=1;"'
        ),
        timeout=300,
    )
    return {"engine": "cassandra", "container": name, "data_dir": data_dir}


def run_mssql(node, data_dir, cfg):
    """Optional — requires accept_eula: true and enough RAM."""
    if not cfg.get("accept_eula"):
        raise RuntimeError("mssql requires accept_eula: true in config")
    name = cfg.get("container_name", "nvmeof-mssql")
    image = _image("mssql", cfg)
    password = cfg.get("password", "RootPassw0rd!")
    port = int(cfg.get("port", 1433))
    duration = int(cfg.get("duration", 60))

    podman_rm(node, name)
    _run(node, f"mkdir -p {data_dir} && chmod 777 {data_dir}")
    _run(
        node,
        (
            f"podman run -d --name {name} "
            f"-e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD={password} "
            f"-v {data_dir}:/var/opt/mssql:Z "
            f"-p {port}:1433 "
            f"{image}"
        ),
        timeout=300,
    )
    if not wait_container_running(node, name):
        raise RuntimeError("MSSQL container failed to start")
    # Wait for SQL ready
    ready = (
        f"podman exec {name} /opt/mssql-tools18/bin/sqlcmd "
        f"-C -S localhost -U sa -P {password} -Q 'SELECT 1'"
    )
    # tools path varies; try without 18
    if not wait_cmd_ok(node, ready, timeout=300):
        ready2 = ready.replace("mssql-tools18", "mssql-tools")
        if not wait_cmd_ok(node, ready2, timeout=120):
            raise RuntimeError("MSSQL not ready")
        ready = ready2

    _run(
        node,
        (
            f"podman exec {name} bash -lc "
            f'"/opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P {password} -Q '
            f"'CREATE DATABASE nvmeofdb;' || "
            f"/opt/mssql-tools/bin/sqlcmd -C -S localhost -U sa -P {password} -Q "
            f"'CREATE DATABASE nvmeofdb;'\""
        ),
        check_ec=False,
        timeout=120,
    )
    # Simple insert loop
    _run(
        node,
        (
            f"podman exec {name} bash -lc "
            f"'end=$((SECONDS+{duration})); n=0; "
            f"while [ $SECONDS -lt $end ]; do "
            f"sqlcmd -C -S localhost -U sa -P {password} -d nvmeofdb "
            f'-Q "IF OBJECT_ID(\\"dbo.kv\\") IS NULL '
            f"CREATE TABLE kv(id INT PRIMARY KEY, v NVARCHAR(64)); "
            f"MERGE kv AS t USING (SELECT $n AS id) AS s ON t.id=s.id "
            f'WHEN MATCHED THEN UPDATE SET v=CONCAT(N\\"u\\", $n) '
            f'WHEN NOT MATCHED THEN INSERT(id,v) VALUES($n, CONCAT(N\\"v\\", $n));" '
            f">/dev/null; n=$((n+1)); done; echo ops=$n'"
        ),
        timeout=duration + 600,
        check_ec=False,
    )
    return {"engine": "mssql", "container": name, "data_dir": data_dir}


ENGINE_RUNNERS = {
    "postgresql": run_postgresql,
    "postgres": run_postgresql,
    "mysql": lambda n, d, c: run_mysql_family(n, d, c, engine="mysql"),
    "mariadb": lambda n, d, c: run_mysql_family(n, d, c, engine="mariadb"),
    "mongodb": run_mongodb,
    "mongo": run_mongodb,
    "redis": run_redis,
    "cassandra": run_cassandra,
    "mssql": run_mssql,
    "sqlserver": run_mssql,
}


def run_database_workload(node, device, cfg):
    """
    Mount device, run one DB engine workload, optionally cleanup container.

    cfg keys:
      engine, mount_point, fstype, duration, cleanup_container (default True)
    """
    engine = (cfg.get("engine") or "").lower()
    if engine in UNSUPPORTED:
        LOG.warning("Skipping %s: %s", engine, UNSUPPORTED[engine])
        return {"engine": engine, "skipped": True, "reason": UNSUPPORTED[engine]}

    if engine not in ENGINE_RUNNERS:
        raise ValueError(f"Unsupported database engine: {engine}")

    mount_point = cfg.get("mount_point") or f"/mnt/nvmeof-db-{engine}"
    data_subdir = cfg.get("data_subdir", "data")
    data_dir = f"{mount_point}/{data_subdir}"
    fstype = cfg.get("fstype", "xfs")

    ensure_podman(node)
    mkfs_and_mount(node, device, mount_point, fstype=fstype)
    _run(node, f"mkdir -p {data_dir}")

    LOG.info("=== DB workload %s on %s (%s) ===", engine, device, data_dir)
    result = ENGINE_RUNNERS[engine](node, data_dir, cfg)

    if cfg.get("cleanup_container", True):
        podman_rm(node, result.get("container", f"nvmeof-{engine}"))
    if cfg.get("umount", True):
        umount_quiet(node, mount_point)

    result["device"] = device
    result["skipped"] = False
    return result
