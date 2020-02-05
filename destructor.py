""" This file is for post-experiment (teardown) and running the drop DDLs on both PostgreSQL and MySQL. """
import argparse
import json

from shared import *


def teardown_postgres(config_directory: str) -> None:
    with open(config_directory + '/general.json', 'r') as general_config_file:
        general_json = json.load(general_config_file)
    with open(config_directory + '/postgres.json', 'r') as postgres_config_file:
        postgres_json = json.load(postgres_config_file)

    try:
        postgres_conn = get_postgres_connection(
            user=postgres_json['user'],
            password=postgres_json['password'],
            host=postgres_json['host'],
            port=int(postgres_json['port'])
        )
        postgres_conn.autocommit = True
        postgres_cur = postgres_conn.cursor()

        with open(general_json['drop-ddl']) as create_ddl_file:
            for statement in create_ddl_file.read().split(';'):
                if not statement.isspace():
                    postgres_cur.execute(statement)

        postgres_cur.execute(f""" DROP DATABASE IF EXISTS {postgres_json['database']}; """)
        postgres_conn.commit()
        postgres_conn.close()

    except Exception as e:
        print('Error in Postgres teardown: ' + str(e))
        exit(1)


def teardown_mysql(config_directory: str) -> None:
    with open(config_directory + '/general.json', 'r') as general_config_file:
        general_json = json.load(general_config_file)
    with open(config_directory + '/mysql.json', 'r') as mysql_config_file:
        mysql_json = json.load(mysql_config_file)

    try:
        mysql_conn = get_mysql_connection(
            user=mysql_json['username'],
            password=mysql_json['password'],
            host=mysql_json['host'],
            database=mysql_json['database']
        )
        mysql_cur = mysql_conn.cursor()

        with open(general_json['drop-ddl']) as create_ddl_file:
            for statement in create_ddl_file.read().split(';'):
                if not statement.isspace():
                    mysql_cur.execute(statement)

        mysql_cur.execute(f""" DROP DATABASE IF EXISTS {mysql_json['database']}; """)
        mysql_conn.commit()
        mysql_conn.close()

    except Exception as e:
        print('Error in MySQL teardown: ' + str(e))
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

    print(f"[{args.database}] experiment database has been destroyed.")
