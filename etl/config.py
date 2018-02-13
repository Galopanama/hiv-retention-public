import os
import subprocess

import psycopg2


def _does_table_exist(curs, table, schema='public'):
    curs.execute("""SELECT EXISTS (
                      SELECT 1
                        FROM information_schema.tables
                       WHERE table_schema = %(schema)s
                         AND table_name = %(table)s);
                  """, {'schema': schema, 'table': table})
    return curs.fetchall()[0][0]


class PostgresConfig(object):
    def __init__(self,
                 host=None,
                 port=5432,
                 database=None,
                 user=None,
                 password=None,
                 luigi_config=None):
        if luigi_config:
            self.host = luigi_config['host']
            self.port = luigi_config['port']
            self.database = luigi_config['database']
            self.user = luigi_config['user']
            self.password = luigi_config['password']
        else:
            self.host = host
            self.port = port
            self.database = database
            self.user = user
            self.password = password

    def as_env_dict(self):
        """For the purposes of setting environment variables, this returns the config
        as a dict with standard variable names.

        :return: Key to value dictionary of environment variables
        :rtype: dict[str, str]
        """
        potential = {
            'PGHOST': self.host,
            'PGUSER': self.user,
            'PGPORT': self.port,
            'PGDATABASE': self.database,
            'PGPASSWORD': self.password
        }
        return {key: str(value) for key, value in potential.items() if value}

    def __enter__(self):
        self.connection = psycopg2.connect(host=self.host,
                                           port=self.port,
                                           database=self.database,
                                           user=self.user,
                                           password=self.password)
        return self.connection.__enter__()

    def __exit__(self, *args):
        self.connection.__exit__(*args)

    def execute_in_psql(self, cmd):
        """Execute a command in psql. This is useful for using the \copy command"""
        env = os.environ.copy()
        env.update(self.as_env_dict())
        p = subprocess.Popen(['psql'], stdin=subprocess.PIPE, env=env)
        p.communicate(input=cmd.encode('utf-8') + b'\n')
        if p.wait() != 0:
            raise subprocess.CalledProcessError('Error in calling psql',
                                                p.returncode,
                                                'psql')

    def does_column_exist(self, table, column, schema='public'):
        """Return whether a particular column exists"""
        with self as conn, conn.cursor() as curs:
            curs.execute("""SELECT EXISTS (
                              SELECT 1
                                FROM information_schema.columns
                               WHERE table_name = %(table)s
                                 AND table_schema = %(schema)s
                                 AND column_name = %(column)s);
                         """, {'table': table, 'schema': schema, 'column': column})
            return curs.fetchall()[0][0]

    def does_table_exist(self, table, schema='public'):
        """Return whether a particular table exists"""
        with self as conn, conn.cursor() as curs:
            return _does_table_exist(curs, table, schema)

    def does_schema_exist(self, schema):
        """Return whether a particular schema exists

        :param str schema: The name of the schema
        :return: Whether it exists
        :rtype: bool
        """
        with self as conn, conn.cursor() as curs:
            curs.execute("""SELECT EXISTS (
                              SELECT 1
                                FROM information_schema.schemata
                               WHERE schema_name = %s);""",
                         (schema,))
            return curs.fetchall()[0][0]

    def create_and_drop(self, table, create_statement, schema='public'):
        with self as conn:
            with conn.cursor() as curs:
                curs.execute("CREATE SCHEMA IF NOT EXISTS " + schema)
                curs.execute("DROP TABLE IF EXISTS {}.{}".format(schema, table))
                curs.execute(create_statement)
            conn.commit()
