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
        postgres_conn_1 = get_postgres_connection(
            user=postgres_json['user'],
            password=postgres_json['password'],
            host=postgres_json['host'],
            port=int(postgres_json['port'])
        )
        postgres_conn_1.autocommit = True
        postgres_conn_1.cursor().execute(f"""CREATE DATABASE {postgres_json['database']};""")
        postgres_conn_1.commit()
        postgres_conn_1.close()

        postgres_conn_2 = get_postgres_connection(
            user=postgres_json['user'],
            password=postgres_json['password'],
            host=postgres_json['host'],
            port=int(postgres_json['port']),
            database=postgres_json['database']
        )
        postgres_cur_2 = postgres_conn_2.cursor()

        with open(resource_directory + '/create.sql') as create_ddl_file:
            for statement in create_ddl_file.read().split(';'):
                if not statement.isspace() and statement != '':
                    postgres_cur_2.execute(statement)
        postgres_conn_2.commit()

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
        mysql_conn_1 = get_mysql_connection(
            user=mysql_json['username'],
            password=mysql_json['password'],
            host=mysql_json['host'],
        )
        mysql_conn_1.cursor().execute(f""" CREATE DATABASE {mysql_json['database']};""")
        mysql_conn_1.commit()
        mysql_conn_1.close()

        mysql_conn_2 = get_mysql_connection(
            user=mysql_json['username'],
            password=mysql_json['password'],
            host=mysql_json['host'],
            database=mysql_json['database']
        )
        mysql_cur_2 = mysql_conn_2.cursor()

        with open(resource_directory + '/create.sql') as create_ddl_file:
            for statement in create_ddl_file.read().split(';'):
                if not statement.isspace():
                    mysql_cur_2.execute(statement)
        mysql_conn_2.commit()

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
