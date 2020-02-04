""" This file is for pre-experiment setup and running the DDLs on both PostgreSQL and MySQL. """
import argparse
import json

from shared import *


def _initialize_postgres(config_directory: str, resource_directory: str) -> None:
    """
    :param config_directory: Location of the 'postgres.json' config file.
    :param resource_directory: Location of the 'create.sql' DDL.
    """
    with open(config_directory + '/postgres.json', 'r') as postgres_config_file:
        postgres_json = json.load(postgres_config_file)

    try:
        postgres_conn = get_postgres_connection(
            user=postgres_json['user'],
            password=postgres_json['password'],
            host=postgres_json['host'],
            port=int(postgres_json['port']),
            database=postgres_json['database']
        )
        postgres_cur = postgres_conn.cursor()

        with open(resource_directory + '/create.sql') as create_ddl_file:
            for statement in create_ddl_file.read().split(';'):
                if statement != '':
                    postgres_cur.execute(statement)
        postgres_conn.commit()

    except Exception as e:
        print('Error in initializing Postgres: ' + str(e))
        exit(1)


def _initialize_mysql(config_directory: str, resource_directory: str) -> None:
    """
    :param config_directory: Location of the 'mysql.json' config file.
    :param resource_directory: Location of the 'create.sql' DDL.
    """
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

        with open(resource_directory + '/create.sql') as create_ddl_file:
            for statement in create_ddl_file.read().split(';'):
                mysql_cur.execute(statement)
        mysql_conn.commit()

    except Exception as e:
        print('Error in initializing MySQL: ' + str(e))
        exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pre-experiment setup and DDL runner for Postgres and MySQL.')
    parser.add_argument('database', type=str, choices=['postgres', 'mysql'], help='Database to initialize.')
    parser.add_argument('--config_path', type=str, default='config', help='Location of configuration files.')
    parser.add_argument('--resource_path', type=str, default='resources', help='Location of DDL files.')
    args = parser.parse_args()

    if args.database == 'postgres':
        _initialize_postgres(args.config_path, args.resource_path)
    else:
        _initialize_mysql(args.config_path, args.resource_path)

    print(f"[{args.database}] has been initialized.")
