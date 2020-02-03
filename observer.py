""" This file holds the task to observe and monitor MySQL and Postgres performance. """
import argparse
import abc
import json
import datetime
import threading
import time

from shared import *

# Global variable to be shared between the main thread and logging thread.
_is_logging_active = False


class _Observer(abc.ABC):
    starting_timestamp = 0

    @staticmethod
    def get_timestamp() -> datetime:
        return datetime.datetime.now()

    @abc.abstractmethod
    def log_action(self) -> None:
        """ Action to performed, represented as a single log message. """
        pass

    @abc.abstractmethod
    def end_logging(self) -> None:
        """ End the logging process (i.e. close all connections). """
        pass

    def log_thread_wrapper(self, frequency: float) -> None:
        """
        Method to be called by the threading instance, to log in a loop.
        :param frequency: Frequency of sampling, measured in actions / minute.
        """
        while True:
            global _is_logging_active

            if not _is_logging_active:
                break

            else:
                self.log_action()
                time.sleep(frequency * 60.)

    def begin_logging(self, is_oneshot: str, frequency: str) -> None:
        """
        :param is_oneshot: Flag which determines if we sample once or multiple times.
        :param frequency: Frequency of sampling, measured in actions / minute.
        """
        self.starting_timestamp = self.get_timestamp()

        if is_oneshot == 'true':
            self.log_action()
            print("Logging has been performed.")

        else:
            global _is_logging_active
            logging_thread = threading.Thread(target=self.log_thread_wrapper, args=(frequency, ))
            _is_logging_active = True
            logging_thread.start()
            input("Press enter to stop logging: ")

            _is_logging_active = False
            logging_thread.join()
            print("Logging has been stopped.")


# noinspection SqlResolve
class _PostgresObserver(_Observer):
    """ https://www.datadoghq.com/blog/postgresql-monitoring/ """

    def __init__(self, results_file: str, user: str, password: str, host: str, port: int, database: str) -> None:
        """
        :param results_file: File to log result tuples to (SQLite).
        :param user: Username to use for PostgreSQL connection.
        :param password: Password to use for PostgreSQL connection.
        :param host: Host URI associated with PostgreSQL connection.
        :param port: Port associated with PostgreSQL connection.
        :param database: PostgreSQL database to use upon connecting.
        """
        # Establish our Postgres connection. Pass any errors up to the factory method.
        self.postgres_conn = get_postgres_connection(user, password, host, port, database)
        self.postgres_cur = self.postgres_conn.cursor()
        self.working_database = database
        self.postgres_cur.execute("""
            SELECT pg_stat_reset();
        """)

        # Establish our results file connection.
        self.results_conn = get_results_connection(results_file=results_file)
        self.results_conn.isolation_level = None
        self.results_cur = self.results_conn.cursor()
        self.results_cur.execute("begin")

        # Create our tables.
        self.results_cur.execute("""
            CREATE TABLE IF NOT EXISTS PostgresStatisticsParent (
                start_of_observation DATETIME NOT NULL,
                measurement_time DATETIME PRIMARY KEY
            );
        """)
        self.results_cur.execute("""
            CREATE TABLE IF NOT EXISTS PostgresStatisticsOnDatabase (
                measurement_time DATETIME,
                temp_bytes INTEGER, -- Data written to temp files by queries. --
                tup_fetched INTEGER, 
                tup_returned INTEGER,
                transactions_committed INTEGER,
                transactions_aborted INTEGER,
                deadlocks INTEGER,
                locks INTEGER,
                checkpoints_requested INTEGER,
                checkpoints_scheduled INTEGER,
                buffers_written_checkpoint INTEGER,
                buffers_written_background INTEGER,
                buffers_written_backends INTEGER,
                shared_buffer_blocks_hit INTEGER,
                shared_buffer_blocks_read INTEGER,
                FOREIGN KEY(measurement_time) REFERENCES PostgresResultsParent(measurement_time)
            );
        """)
        self.results_cur.execute("""
            CREATE TABLE IF NOT EXISTS PostgresResultsOnTable (
                measurement_time DATETIME,
                relation_name TEXT,
                sequential_scans INTEGER,
                tuples_fetched_from_seq INTEGER,
                index_scans INTEGER,
                tuples_fetched_from_idx INTEGER,
                tuples_inserted INTEGER,
                live_tuples INTEGER, -- Tuples available to be read / modified. --
                dead_tuples INTEGER, -- Tuples that can be overwritten in the future. --
                shared_buffer_blocks_hit INTEGER,
                shared_buffer_blocks_read INTEGER,
                shared_buffer_idx_blocks_hit INTEGER,
                shared_buffer_idx_blocks_read INTEGER,
                FOREIGN KEY(measurement_time) REFERENCES PostgresResultsParent(measurement_time)
            );
        """)

    def log_action(self) -> None:
        # Perform a sample.
        self.postgres_cur.execute(f"""
            SELECT ST.temp_bytes, ST.tup_fetched, ST.tup_returned, ST.xact_commit, ST. xact_rollback, ST.deadlocks,
                   LK.lock_count, BW.checkpoints_req, BW.checkpoints_timed, BW.buffers_checkpoint, BW.buffers_clean,
                   BW.buffers_backend, ST.blks_hit, ST.blks_read
            FROM pg_stat_database AS ST 
            CROSS JOIN pg_stat_bgwriter AS BW
            CROSS JOIN (
                SELECT COUNT(*) AS lock_count
                FROM pg_locks LKI INNER JOIN pg_database DBI
                ON LKI.database = DBI.oid
                WHERE DBI.datname = '{self.working_database}'
            ) AS LK
            WHERE ST.datname = '{self.working_database}'
            ORDER BY ST.stats_reset DESC,
                     BW.stats_reset;
        """)
        on_database_results = self.postgres_cur.fetchone()
        self.postgres_cur.execute(f"""
            SELECT ST.relname, ST.seq_scan, ST.seq_tup_read, COALESCE(ST.idx_scan, 0), COALESCE(ST.idx_tup_fetch, 0), 
                   ST.n_tup_ins, ST.n_live_tup, ST.n_dead_tup, IO.heap_blks_hit, IO.heap_blks_read,
                   COALESCE(IO.idx_blks_hit, 0), COALESCE(IO.idx_blks_read, 0)
            FROM pg_stat_user_tables AS ST
            INNER JOIN pg_statio_user_tables AS IO
            ON ST.relid = IO.relid
            LIMIT 1;
        """)
        on_tables_results = self.postgres_cur.fetchall()

        # ... and log the sample.
        sample_timestamp = self.get_timestamp()
        self.results_cur.execute("""
            INSERT INTO PostgresStatisticsParent
            VALUES (?, ?)
        """, [self.starting_timestamp, sample_timestamp])
        self.results_cur.execute(f"""
            INSERT INTO PostgresStatisticsOnDatabase
            VALUES ({','.join('?' for _ in range(len(on_database_results) + 1))})
        """, [sample_timestamp] + list(on_database_results))
        self.results_cur.executemany(f"""
            INSERT INTO PostgresResultsOnTable
            VALUES ({','.join('?' for _ in range(len(on_tables_results[0]) + 1))})
        """, list(map(lambda a: [sample_timestamp] + list(a), on_tables_results)))

    def end_logging(self) -> None:
        self.results_cur.execute('commit')
        self.results_conn.commit()

        self.results_conn.close()
        self.postgres_conn.close()


# noinspection SqlResolve
class _MySQLObserver(_Observer):
    """ https://www.datadoghq.com/blog/collecting-mysql-statistics-and-metrics/ """

    def __init__(self, results_file: str, user: str, password: str, host: str) -> None:
        """
        :param results_file: File to log result tuples to (SQLite).
        :param user: Username to use for connection.
        :param password: Password to use for connection.
        :param host: Host URI associated with connection.
        """
        # Establish our MySQL connection. Pass any errors up to the factory method.
        self.mysql_conn = get_mysql_connection(user, password, host, 'sys')
        self.postgres_cur = self.mysql_conn.cursor()

        # Establish our results file connection.
        self.results_conn = get_results_connection(results_file=results_file)
        self.results_conn.isolation_level = None
        self.results_cur = self.results_conn.cursor()
        self.results_cur.execute("begin")

        # Create our tables.
        self.results_cur.execute("""
            CREATE TABLE IF NOT EXISTS MySQLStatisticsParent (
                start_of_observation DATETIME NOT NULL,
                measurement_time DATETIME PRIMARY KEY
            );
        """)
        self.results_cur.execute("""
            CREATE TABLE IF NOT EXISTS MySQLStatistics
        """) # TODO: FINISH THE MYSQL STATS SCHEMA

    def log_action(self) -> None: # TODO: FINISH THE MYSQL LOGGING
        pass

    def end_logging(self) -> None:
        self.results_cur.execute('commit')
        self.results_conn.commit()

        self.results_conn.close()
        self.mysql_conn.close()


def _observer_factory(config_directory: str, database_option: str) -> _Observer:
    """
    :param config_directory: Location of the 'results.json', 'postgres.json', and 'mysql.json' config files.
    :param database_option: Type of observer to producer.
    :return: A _Database instance, dependent on the database_option.
    """

    with open(config_directory + '/results.json', 'r') as results_config_file:
        results_json = json.load(results_config_file)

    if database_option == 'postgres':
        with open(config_directory + '/postgres.json', 'r') as postgres_config_file:
            postgres_json = json.load(postgres_config_file)

        try:
            return _PostgresObserver(
                results_file=results_json['observation-db'],
                user=postgres_json['user'],
                password=postgres_json['password'],
                host=postgres_json['host'],
                port=int(postgres_json['port']),
                database=postgres_json['database']
            )
        except Exception as e:
            print(e)
            exit(1)

    else:
        with open(config_directory + '/mysql.json', 'r') as mysql_config_file:
            mysql_json = json.load(mysql_config_file)

        try:
            return _MySQLObserver(
                results_file=results_json['observation-db'],
                user=mysql_json['user'],
                password=mysql_json['password'],
                host=mysql_json['host']
            )
        except Exception as e:
            print(e)
            exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Non-invasively collects data from the experiment runner.')

    help_strings = {
        "database": 'Which database to run experiments on.',
        "oneshot": 'If true, we collect our statistics once. Otherwise, we run our experiments until user input.',
        "frequency": 'Frequency of sampling, measured in actions / minute. Only applies if oneshot is false.',
        "config_path": 'Location of configuration files.'
    }
    parser.add_argument('database', type=str, choices=['postgres', 'mysql'], help=help_strings['database'])
    parser.add_argument('oneshot', type=str, choices=['true', 'false'], help=help_strings['oneshot'])
    parser.add_argument('--frequency', type=float, help=help_strings['frequency'])
    parser.add_argument('--config_path', type=str, default='config', help=help_strings['config_path'])
    args = parser.parse_args()

    observer = _observer_factory(args.config_path, args.database)
    observer.begin_logging(args.oneshot, args.frequency)
    observer.end_logging()
