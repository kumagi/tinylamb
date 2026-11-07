"""Optional Oracle Free backend; all services and credentials are disposable."""

import datetime
import os
import re
import secrets
import subprocess
import time


# Rewrite the closed benchmark template vocabulary without touching literals.
TOKENS = re.compile(r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|--[^\n]*|/\*.*?\*/|[A-Za-z_][A-Za-z_0-9]*|\d+|[^\w\s]|\s+", re.S)


def adapt_sql(sql):
    sql = sql.strip().removesuffix(';')
    tokens = TOKENS.findall(sql)
    # These templates use a terminal LIMIT only, never one inside a subquery.
    tail = re.search(r'\bLIMIT\s+(\d+)\s*$', sql, re.I)
    if tail:
        sql = sql[:tail.start()] + 'FETCH FIRST ' + tail[1] + ' ROWS ONLY'
        tokens = TOKENS.findall(sql)
    result = []
    i = 0
    while i < len(tokens):
        token = tokens[i]
        upper = token.upper()
        if upper == 'BIGINT':
            result.append('NUMBER(19)')
        elif upper == 'DOUBLE' and i + 2 < len(tokens) and tokens[i + 1].isspace() and tokens[i + 2].upper() == 'PRECISION':
            result.append('BINARY_DOUBLE')
            i += 2
        elif upper == 'VARCHAR':
            result.append('VARCHAR2')
        else:
            result.append(token)
        i += 1
    return ''.join(result)


class Client:
    def __init__(self, ports, timeout=30):
        import oracledb
        self.engine = 'oracle'
        self.date_type = oracledb.DB_TYPE_DATE
        self.connection = oracledb.connect(
            user='bench', password=os.environ['TINYLAMB_BENCH_ORACLE_PASSWORD'],
            host='127.0.0.1', port=ports['oracle'], service_name='FREEPDB1')
        self.connection.call_timeout = timeout * 1000
        self.connection.autocommit = True
        # Disable statement caching: the other backends use non-prepared SQL.
        self.connection.stmtcachesize = 0
        self.cursor = self.connection.cursor()
        self.cursor.arraysize = 100
        self.explicit_transaction = False
        self.cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'")
        self.cursor.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'")
        self.cursor.execute("ALTER SESSION SET ISOLATION_LEVEL = READ COMMITTED")
        self.cursor.execute("ALTER SESSION SET COMMIT_WAIT = WAIT")
        self.cursor.execute("ALTER SESSION SET COMMIT_LOGGING = IMMEDIATE")

    def execute(self, sql):
        sql = adapt_sql(sql)
        if sql.upper() == 'BEGIN':
            if self.explicit_transaction:
                raise RuntimeError('nested benchmark transaction')
            self.connection.autocommit = False
            self.explicit_transaction = True
            return []
        if sql.upper() in ('COMMIT', 'ROLLBACK'):
            if sql.upper() == 'COMMIT':
                self.connection.commit()
            else:
                self.connection.rollback()
            self.connection.autocommit = True
            self.explicit_transaction = False
            return []
        if sql.upper().startswith('EXPLAIN '):
            self.cursor.execute('EXPLAIN PLAN FOR ' + sql[len('EXPLAIN '):])
            self.cursor.execute("SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY(NULL, NULL, 'BASIC +PREDICATE +COST'))")
        else:
            self.cursor.execute(sql)
        if not self.cursor.description:
            return []
        # Oracle DATE includes time; fixtures contain date-only values.
        date_columns = [i for i, d in enumerate(self.cursor.description)
                        if d.type_code == self.date_type]
        rows = self.cursor.fetchall()
        return [tuple(v.date() if i in date_columns and isinstance(v, datetime.datetime)
                      and v.time() == datetime.time() else v for i, v in enumerate(row)) for row in rows]

    def load_batch(self, table, rows):
        placeholders = ','.join(f':{i + 1}' for i in range(len(rows[0])))
        self.cursor.executemany(f'INSERT INTO {table} VALUES ({placeholders})', rows)

    def analyze(self, table):
        self.cursor.execute('BEGIN DBMS_STATS.GATHER_TABLE_STATS(USER, :1, cascade=>TRUE); END;', [table.upper()])

    def close(self):
        self.cursor.close()
        self.connection.close()


class Container:
    def __init__(self, args, work, record_command):
        self.args, self.work, self.record = args, work, record_command
        self.name = f'tinylamb-comparison-oracle-{os.getpid()}'
        self.started = False

    def start(self):
        password = 'Bench_' + secrets.token_hex(16)
        os.environ['TINYLAMB_BENCH_ORACLE_PASSWORD'] = password
        env = self.work / 'oracle.env'
        env.touch(mode=0o600)
        env.write_text('ORACLE_PWD=' + password + '\nENABLE_ARCHIVELOG=false\nENABLE_FORCE_LOGGING=true\n')
        argv = ['docker', 'run', '-d', '--name', self.name,
                '--cpuset-cpus', self.args.server_cpus, '--shm-size=2g',
                '-p', f'127.0.0.1:{self.args.oracle_port}:1521',
                '--env-file', str(env), self.args.oracle_image]
        self.record(argv)
        subprocess.check_output(argv, text=True)
        self.started = True
        deadline = time.monotonic() + 900
        while True:
            ready = subprocess.check_output(['docker', 'inspect', '--format', '{{.State.Health.Status}}', self.name], text=True).strip()
            if ready == 'healthy':
                break
            if time.monotonic() > deadline:
                raise RuntimeError('Oracle Free did not become healthy in 900 seconds')
            time.sleep(2)
        # The prebuilt Full database can already be in ARCHIVELOG mode. The
        # environment switch alone does not undo that state. Explicitly match
        # the non-archiving PG/MySQL profiles, keeping redo + WAIT commits.
        self.setup_output = subprocess.check_output(
            ['docker', 'exec', '-i', self.name, 'sqlplus', '-s', '/', 'as', 'sysdba'],
            input='''WHENEVER SQLERROR EXIT SQL.SQLCODE
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE NOARCHIVELOG;
ALTER DATABASE OPEN;
ALTER PLUGGABLE DATABASE ALL OPEN;
EXIT
''', text=True)
        # No host installation, cloud account, or persistent shared credentials.
        # The SQL is sent over stdin rather than written to provenance logs.
        sql = f'''WHENEVER SQLERROR EXIT SQL.SQLCODE
ALTER SESSION SET CONTAINER=FREEPDB1;
CREATE USER bench IDENTIFIED BY "{password}" DEFAULT TABLESPACE users QUOTA UNLIMITED ON users;
GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE PROCEDURE TO bench;
GRANT SELECT ON v_$parameter TO bench;
GRANT SELECT ON v_$version TO bench;
GRANT SELECT ON v_$database TO bench;
GRANT SELECT ON v_$sgainfo TO bench;
EXIT
'''
        subprocess.run(['docker', 'exec', '-i', self.name, 'sqlplus', '-s', '/', 'as', 'sysdba'],
                       input=sql, text=True, check=True, stdout=subprocess.DEVNULL)

