""" This file holds the task to observe and monitor MySQL and Postgres performance. """
from shared import get_mysql_connection, get_postgres_connection


def mysql_observer (**kwargs):
    conn = get_mysql_connection(**kwargs)


def postgres_observer(**kwargs):
    conn = get_postgres_connection(**kwargs)