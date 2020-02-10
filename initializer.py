""" This file is for pre-experiment setup and running the DDLs and metadata inserts on both PostgreSQL and MySQL. """
from connect import get_mysql_new_connection, get_postgres_new_connection

import argparse
import json


def initialize_postgres(config_directory: str) -> None:
    """ https://dev.to/pythonmeister/basic-postgresql-tuning-parameters-281 """

    with open(config_directory + '/general.json', 'r') as general_config_file:
        general_json = json.load(general_config_file)
    with open(config_directory + '/postgres.json', 'r') as postgres_config_file:
        postgres_json = json.load(postgres_config_file)

    try:
        # Create the database.
        postgres_conn_1 = get_postgres_new_connection(
            user=postgres_json['user'],
            password=postgres_json['password'],
            host=postgres_json['host']
        )
        postgres_conn_1.autocommit = True
        postgres_conn_1.cursor().execute(f""" CREATE DATABASE {postgres_json['database']};""")
        postgres_conn_1.cursor().execute(f""" ALTER SYSTEM SET track_io_timing = on; """)
        postgres_conn_1.cursor().execute(f""" ALTER SYSTEM SET log_statement_stats = on; """)
        postgres_conn_1.cursor().execute(f""" ALTER SYSTEM SET log_executor_stats = on; """)
        postgres_conn_1.cursor().execute(f""" ALTER SYSTEM SET max_connections = 1000; """)  # Hard coded!!
        postgres_conn_1.cursor().execute(f""" ALTER SYSTEM SET max_prepared_transactions = 1000; """)
        postgres_conn_1.close()

        # Create the tables.
        postgres_conn_2 = get_postgres_new_connection(
            user=postgres_json['user'],
            password=postgres_json['password'],
            host=postgres_json['host'],
            database=postgres_json['database']
        )
        postgres_conn_2.autocommit = True
        postgres_cur_2 = postgres_conn_2.cursor()
        with open(general_json['create-ddl']) as create_ddl_file:
            for statement in create_ddl_file.read().split(';'):
                if not statement.isspace() and statement != '':
                    postgres_cur_2.execute(statement)
        postgres_cur_2.close()

    except Exception as e:
        print('Error in initializing Postgres: ' + str(e))
        exit(1)


def initialize_mysql(config_directory: str) -> None:
    with open(config_directory + '/general.json', 'r') as general_config_file:
        general_json = json.load(general_config_file)
    with open(config_directory + '/mysql.json', 'r') as mysql_config_file:
        mysql_json = json.load(mysql_config_file)

    try:
        # Create the database.
        mysql_conn_1 = get_mysql_new_connection(
            user=mysql_json['username'],
            password=mysql_json['password'],
            host=mysql_json['host'],
        )
        mysql_conn_1.cursor().execute(f""" CREATE DATABASE {mysql_json['database']};""")
        mysql_conn_1.cursor().execute(f""" SET PERSIST innodb_thread_concurrency = 1000; """)  # Hard coded!!
        mysql_conn_1.cursor().execute(f""" SET PERSIST max_connections = 1000; """)
        mysql_conn_1.commit()
        mysql_conn_1.close()

        # Create the tables.
        mysql_conn_2 = get_mysql_new_connection(
            user=mysql_json['username'],
            password=mysql_json['password'],
            host=mysql_json['host'],
            database=mysql_json['database']
        )
        mysql_cur_2 = mysql_conn_2.cursor()
        with open(general_json['create-ddl']) as create_ddl_file:
            for statement in create_ddl_file.read().split(';'):
                if not statement.isspace():
                    mysql_cur_2.execute(statement)
        mysql_conn_2.commit()
        mysql_conn_2.close()

    except Exception as e:
        print('Error in initializing MySQL: ' + str(e))
        exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pre-experiment setup and DDL runner for Postgres and MySQL.')
    parser.add_argument('database', type=str, choices=['postgres', 'mysql'], help='Database to initialize.')
    parser.add_argument('--config_path', type=str, default='config', help='Location of configuration files.')
    args = parser.parse_args()

    if args.database == 'postgres':
        initialize_postgres(args.config_path)
    else:
        initialize_mysql(args.config_path)

    print(f"[{args.database}] has been initialized.")
