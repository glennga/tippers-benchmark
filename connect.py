""" This file holds all connect-based methods. """

import mysql.connector.pooling
import mysql.connector
import psycopg2.pool
import psycopg2
import sqlite3

# We maintain a connection pool singleton for both Postgres and MySQL.
_mysql_connection_pool = None
_postgres_connection_pool = None


def get_mysql_new_connection(user: str, password: str, host: str, database: str = None):
    """
    :param user: Username to use for connection.
    :param password: Password to use for connection.
    :param host: Host URI associated with connection.
    :param database: MySQL database to use upon connecting.
    :return: A connection to some MySQL database.
    """
    if database is not None:
        return mysql.connector.connect(
            user=user,
            passwd=password,
            host=host,
            database=database
        )
    else:
        return mysql.connector.connect(
            user=user,
            passwd=password,
            host=host
        )


def get_mysql_pooled_connection(user: str, password: str, host: str, database: str, **kwargs):
    """
    :param user: Username to use for connection.
    :param password: Password to use for connection.
    :param host: Host URI associated with connection.
    :param database: MySQL database to use upon connecting.
    :param kwargs: If this is the first case, 'pool_size' MUST be specified.
    :return: A connection to some MySQL database.
    """
    global _mysql_connection_pool

    if _mysql_connection_pool is None and 'pool_size' not in kwargs:
        raise ConnectionRefusedError('Must specify pool_size for first call to get pooled connection.')

    elif _mysql_connection_pool is None:
        _mysql_connection_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="TippersConnectionPool",
            pool_size=kwargs['pool_size'] + 1,
            user=user,
            passwd=password,
            host=host,
            database=database
        )

    return _mysql_connection_pool.get_connection()


def get_postgres_new_connection(user: str, password: str, host: str, database: str = None):
    """
    :param user: Username to use for connection.
    :param password: Password to use for connection.
    :param host: Host URI associated with connection.
    :param database: PostgreSQL database to use upon connecting.
    :return: A connection to some PostgreSQL database.
    """
    if database is not None:
        return psycopg2.connect(
            user=user,
            password=password,
            host=host,
            database=database
        )
    else:
        return psycopg2.connect(
            user=user,
            password=password,
            host=host
        )


def get_postgres_pooled_connection(user: str, password: str, host: str, database: str, **kwargs):
    """
    :param user: Username to use for connection.
    :param password: Password to use for connection.
    :param host: Host URI associated with connection.
    :param database: PostgreSQL database to use upon connecting.
    :param kwargs: If this is the first case, 'pool_size' MUST be specified.
    :return: A connection to some PostgreSQL database.
    """
    global _postgres_connection_pool

    if _postgres_connection_pool is None and 'pool_size' not in kwargs:
        raise ConnectionRefusedError('Must specify pool_size for first call to get pooled connection.')

    elif _postgres_connection_pool is None:
        _postgres_connection_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=kwargs['pool_size'] + 1,
            user=user,
            password=password,
            host=host,
            database=database
        )

    return _postgres_connection_pool.getconn()


def get_results_connection(results_file: str):
    """
    :param results_file: File to create / append to.
    :return: A connection to some SQLite database.
    """
    return sqlite3.connect(
        results_file,
        check_same_thread=False
    )
