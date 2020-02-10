""" This file holds all shared methods between any two files. """

import mysql.connector as mysql
import psycopg2 as postgres
import sqlite3


def get_mysql_connection(user: str, password: str, host: str, database: str = None):
    """
    :param user: Username to use for connection.
    :param password: Password to use for connection.
    :param host: Host URI associated with connection.
    :param database: MySQL database to use upon connecting.
    :return: A connection to some MySQL database.
    """
    if database is not None:
        return mysql.connect(
            user=user,
            passwd=password,
            host=host,
            database=database
        )
    else:
        return mysql.connect(
            user=user,
            passwd=password,
            host=host
        )


def get_postgres_connection(user: str, password: str, host: str, database: str = None):
    """
    :param user: Username to use for connection.
    :param password: Password to use for connection.
    :param host: Host URI associated with connection.
    :param database: PostgreSQL database to use upon connecting.
    :return: A connection to some PostgreSQL database.
    """
    if database is not None:
        return postgres.connect(
            user=user,
            password=password,
            host=host,
            database=database
        )
    else:
        return postgres.connect(
            user=user,
            password=password,
            host=host
        )


def get_results_connection(results_file: str):
    """
    :param results_file: File to create / append to.
    :return: A connection to some SQLite database.
    """
    return sqlite3.connect(
        results_file,
        check_same_thread=False
    )
