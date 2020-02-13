""" This file is for post-experiment (teardown) and running the drop DDLs on both PostgreSQL and MySQL. """
from connect import get_mysql_new_connection, get_postgres_new_connection

import datetime
import argparse
import json


def teardown_postgres(config_directory: str, is_partial: bool = False) -> None:
    with open(config_directory + '/general.json', 'r') as general_config_file:
        general_json = json.load(general_config_file)
    with open(config_directory + '/postgres.json', 'r') as postgres_config_file:
        postgres_json = json.load(postgres_config_file)

    try:
        postgres_conn = get_postgres_new_connection(
            user=postgres_json['user'],
            password=postgres_json['password'],
            host=postgres_json['host']
        )
        postgres_conn.autocommit = True
        postgres_cur = postgres_conn.cursor()

        # Kick off every other user to this database.
        postgres_cur.execute(f"""
            SELECT pg_terminate_backend(pid) 
            FROM pg_stat_activity 
            WHERE datname = '{postgres_json['database']}' AND pid <> pg_backend_pid();
        """)

        with open(general_json['drop-ddl'] if not is_partial else general_json['partial-drop-ddl']) as create_ddl_file:
            for statement in create_ddl_file.read().split(';'):
                if not statement.isspace():
                    try:
                        postgres_cur.execute(statement)
                    except Exception as e:
                        print(f'[{datetime.datetime.now()}][destructor.py] Error at: {e}')

        if not is_partial:
            postgres_cur.execute(f""" DROP DATABASE IF EXISTS {postgres_json['database']}; """)

        postgres_cur.close()
        postgres_conn.close()
        print(f'[{datetime.datetime.now()}][destructor.py] {"Partial" if is_partial else "Complete"} teardown '
              f'has been performed for Postgres.')

    except Exception as e:
        print(f'[{datetime.datetime.now()}][destructor.py] Error in Postgres teardown: ' + str(e))
        exit(1)


def teardown_mysql(config_directory: str, is_partial: bool = False) -> None:
    with open(config_directory + '/general.json', 'r') as general_config_file:
        general_json = json.load(general_config_file)
    with open(config_directory + '/mysql.json', 'r') as mysql_config_file:
        mysql_json = json.load(mysql_config_file)

    try:
        mysql_conn = get_mysql_new_connection(
            user=mysql_json['username'],
            password=mysql_json['password'],
            host=mysql_json['host'],
            database=mysql_json['database']
        )
        mysql_cur = mysql_conn.cursor()

        with open(general_json['drop-ddl'] if not is_partial else general_json['partial-drop-ddl']) as create_ddl_file:
            for statement in create_ddl_file.read().split(';'):
                if not statement.isspace():
                    try:
                        mysql_cur.execute(statement)
                    except Exception as e:
                        print(f'[{datetime.datetime.now()}][destructor.py] Error at: {e}')

        if not is_partial:
            mysql_cur.execute(f""" DROP DATABASE IF EXISTS {mysql_json['database']}; """)

        mysql_conn.commit()
        mysql_conn.close()
        print(f'[{datetime.datetime.now()}][destructor.py] {"Partial" if is_partial else "Complete"} teardown '
              f'has been performed for MySQL.')

    except Exception as e:
        print(f'[{datetime.datetime.now()}][destructor.py] Error in MySQL teardown: ' + str(e))
        exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Post-experiment teardown and drop DDL runner for Postgres and MySQL.')
    parser.add_argument('database', type=str, choices=['postgres', 'mysql'], help='Database to drop tables for.')
    parser.add_argument('--config_path', type=str, default='config', help='Location of configuration files.')
    args = parser.parse_args()

    if args.database == 'postgres':
        teardown_postgres(args.config_path)
    else:
        teardown_mysql(args.config_path)

    print(f"[{datetime.datetime.now()}][destructor.py] Database {args.database} has been destroyed.")
