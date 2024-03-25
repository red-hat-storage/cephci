import psycopg


class Grafana:
    def __init__(self, db_name, db_user, db_pwd, db_host, db_port):
        self.db_name = db_name
        self.db_user = db_user
        self.db_pwd = db_pwd
        self.db_host = db_host
        self.db_port = db_port
        self.db_conn = self._establish_db_conn()

    def _establish_db_conn(self):
        return psycopg.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_pwd,
            host=self.db_host,
            port=self.db_port,
        )
        self.db_conn.autocommit = True

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
        self.db_conn.cursor().execute(cmd)
        self.db_conn.commit()
