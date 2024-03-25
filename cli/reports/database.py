import psycopg

from cli.exceptions import ConfigError


class Database:
    def __init__(self, **creds):
        """Initialize instance using provided details

        **Kwargs:
            name(str): Name of DB
            user(str): Username
            pwd(str): db Password
            host(str): host ip/ hostname
            port(str): Port no.
            table(str): db Table name
        """
        try:
            self.name = creds["name"]
            self.user = creds["user"]
            self.pwd = creds["pwd"]
            self.host = creds["host"]
            self.port = creds["port"]
        except KeyError:
            raise ConfigError("Mandatory parameters are missing")
        self.conn = self._establish_db_conn()

    def _establish_db_conn(self):
        return psycopg.connect(
            dbname=self.name,
            user=self.user,
            password=self.pwd,
            host=self.host,
            port=self.port,
        )
        self.conn.autocommit = True

    def insert(self, table, cols, data):
        insert_query = f"""INSERT INTO {table} ({cols}) VALUES ({data})"""
        self._execute(insert_query)

    def update(self):
        update_query = """<cmd here>"""
        self._execute(update_query)

    def delete(self):
        delete_query = """<cmd here>"""
        self._execute(delete_query)

    def _execute(self, cmd):
        self.conn.cursor().execute(cmd)
        self.conn.commit()
