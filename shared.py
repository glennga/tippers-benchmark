""" This file holds all shared methods between any two files. """

import mysql.connector as mysql
import psycopg2 as postgres


def get_mysql_connection(**kwargs):
    return mysql.connect(
        user=kwargs['user'],
        passwd=kwargs['passwrd'],
        host=kwargs['host']
    )


def get_postgres_connection(**kwargs):
    return postgres.connect(
        user=kwargs['user'],
        password=kwargs['password'],
        host=kwargs['host'],
        port=kwargs['port'],
        database=kwargs['database']
    )
