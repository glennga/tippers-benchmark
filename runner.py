""" This file is the Python entry point to launch an experiment and observer. """
import argparse
import functools
import json
import abc
from typing import Callable, Dict

from initializer import initialize_mysql, initialize_postgres
from destructor import teardown_mysql, teardown_postgres
from simulator import process_transactions
from shared import *


class _GenericExperimentFactory(abc.ABC):
    @abc.abstractmethod
    def _mpl_decorator(self, func, working_mpl: int):
        pass

    @abc.abstractmethod
    def _experiment_t(self, isolation: str, file_locations: Dict[str, str], config_path: str):
        pass

    @abc.abstractmethod
    def _experiment_q(self, isolation: str, file_locations: Dict[str, str], config_path: str):
        pass

    @abc.abstractmethod
    def _experiment_w(self, isolation: str, file_locations: Dict[str, str], config_path: str):
        pass

    def __call__(self, experiment: str) -> Callable:
        if experiment == 't':
            return lambda c, f, m, i: self._mpl_decorator(self._experiment_t, m)(i, f, c)
        elif experiment == 'q':
            return lambda c, f, m, i: self._mpl_decorator(self._experiment_q, m)(i, f, c)
        else:
            return lambda c, f, m, i: self._mpl_decorator(self._experiment_w, m)(i, f, c)


class _PostgresExperimentFactory(_GenericExperimentFactory):
    def __init__(self, postgres_json, concurrency: str):
        self.postgres_json = postgres_json
        self.concurrency = concurrency

    def _mpl_decorator(self, func, working_mpl: int):
        @functools.wraps(func)
        def _mpl_wrapper(*args, **kwargs):
            conn = get_postgres_connection(
                user=self.postgres_json['user'],
                password=self.postgres_json['password'],
                host=self.postgres_json['host'],
                database=self.postgres_json['database']
            )
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(f""" ALTER SYSTEM SET max_connections = {working_mpl}; """)
            cur.execute(f""" ALTER SYSTEM SET max_prepared_transactions = {working_mpl}; """)
            conn.close()

            func(*args, **kwargs)

        return _mpl_wrapper

    def _experiment_tw(self, isolation: str, file_locations: Dict[str, str], config_path: str, exp_no: int):
        teardown_postgres(config_path)  # Each experiment is contained. We must teardown then initialize again.
        initialize_postgres(config_path)

        conn = get_postgres_connection(
            user=self.postgres_json['user'],
            password=self.postgres_json['password'],
            host=self.postgres_json['host'],
            database=self.postgres_json['database']
        )
        cur = conn.cursor()

        # Insert metadata.
        with open(general_json[f'data-{self.concurrency}-concurrency-metadata']) as insert_metadata_file:
            statement = insert_metadata_file.readline()
            while statement:
                cur.execute(statement)
                statement = insert_metadata_file.readline()

        conn.commit()
        conn.close()

        # Determine the isolation level.
        if isolation == 'ru':
            postgres_isolation = 0
        elif isolation == 'rc':
            postgres_isolation = 1
        elif isolation == 'rr':
            postgres_isolation = 2
        else:
            postgres_isolation = 3

        process_transactions(
            file_name=file_locations[f'data-{self.concurrency}-concurrency-observations'],
            hostname=self.postgres_json['host'],
            username=self.postgres_json['user'],
            password=self.postgres_json['password'],
            database=self.postgres_json['database'],
            is_mysql=False,
            iso_level=postgres_isolation,
            exp_no=exp_no
        )

    def _experiment_t(self, isolation: str, file_locations: Dict[str, str], config_path: str):
        return self._experiment_tw(isolation, file_locations, config_path, 1)

    def _experiment_q(self, isolation: str, file_locations: Dict[str, str], config_path: str):
        if isolation == 'ru':
            postgres_isolation = 0
        elif isolation == 'rc':
            postgres_isolation = 1
        elif isolation == 'rr':
            postgres_isolation = 2
        else:
            postgres_isolation = 3

        process_transactions(
            file_name=file_locations[f'data-{self.concurrency}-concurrency-observations'],
            hostname=self.postgres_json['host'],
            username=self.postgres_json['user'],
            password=self.postgres_json['password'],
            database=self.postgres_json['database'],
            is_mysql=False,
            iso_level=postgres_isolation,
            exp_no=2
        )

    def _experiment_w(self, isolation: str, file_locations: Dict[str, str], config_path: str):
        return self._experiment_tw(isolation, file_locations, config_path, 3)


class _MySQLExperimentFactory(_GenericExperimentFactory):
    def __init__(self, mysql_json, concurrency: str):
        self.mysql_json = mysql_json
        self.concurrency = concurrency

    def _mpl_decorator(self, func, working_mpl: int):
        @functools.wraps(func)
        def _mpl_wrapper(*args, **kwargs):
            conn = get_mysql_connection(
                user=self.mysql_json['username'],
                password=self.mysql_json['password'],
                host=self.mysql_json['host'],
                database=self.mysql_json['database']
            )
            cur = conn.cursor()
            cur.execute(f""" SET PERSIST innodb_thread_concurrency = {working_mpl}; """)
            conn.commit()
            conn.close()

            func(*args, **kwargs)

        return _mpl_wrapper

    def _experiment_tw(self, isolation: str, file_locations: Dict[str, str], config_path, exp_no: int):
        teardown_mysql(config_path)  # Each experiment is contained. We must teardown then initialize again.
        initialize_mysql(config_path)

        conn = get_mysql_connection(
            user=self.mysql_json['username'],
            password=self.mysql_json['password'],
            host=self.mysql_json['host'],
            database=self.mysql_json['database']
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Insert metadata.
        with open(general_json[f'data-{self.concurrency}-concurrency-metadata']) as insert_metadata_file:
            statement = insert_metadata_file.readline()
            while statement:
                cur.execute(statement)
                statement = insert_metadata_file.readline()

        conn.close()

        # Determine the isolation level.
        if isolation == 'ru':
            mysql_isolation = 'READ UNCOMMITTED'
        elif isolation == 'rc':
            mysql_isolation = 'READ COMMITTED'
        elif isolation == 'rr':
            mysql_isolation = 'REPEATABLE READ'
        else:
            mysql_isolation = 'SERIALIZABLE'

        process_transactions(
            file_name=file_locations[f'data-{self.concurrency}-concurrency-observations'],
            hostname=self.mysql_json['host'],
            username=self.mysql_json['username'],
            password=self.mysql_json['password'],
            database=self.mysql_json['database'],
            is_mysql=True,
            iso_level=mysql_isolation,
            exp_no=exp_no
        )

    def _experiment_t(self, isolation: str, file_locations: Dict[str, str], config_path):
        return self._experiment_tw(isolation, file_locations, config_path, 1)

    def _experiment_q(self, isolation: str, file_locations: Dict[str, str], config_path):
        if isolation == 'ru':
            mysql_isolation = 'READ UNCOMMITTED'
        elif isolation == 'rc':
            mysql_isolation = 'READ COMMITTED'
        elif isolation == 'rr':
            mysql_isolation = 'REPEATABLE READ'
        else:
            mysql_isolation = 'SERIALIZABLE'

        process_transactions(
            file_name=file_locations[f'data-{self.concurrency}-concurrency-observations'],
            hostname=self.mysql_json['host'],
            username=self.mysql_json['username'],
            password=self.mysql_json['password'],
            database=self.mysql_json['database'],
            is_mysql=True,
            iso_level=mysql_isolation,
            exp_no=2
        )

    def _experiment_w(self, isolation: str, file_locations: Dict[str, str], config_path):
        return self._experiment_tw(isolation, file_locations, config_path, 3)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run experiments on the tipper\'s benchmark.')

    help_strings = {
        "database": 'Which database to run experiments on.',
        "experiment": "Which experiment to run. t=throughput, q=query, w=workload.",
        "concurrency": 'Type of concurrency experiment to run.',
        "mpl": 'Multiprogramming level to run.',
        "isolation": "Isolation level to run.",
        "config_path": 'Location of configuration files.'
    }
    parser.add_argument('database', type=str, choices=['postgres', 'mysql'], help=help_strings['database'])
    parser.add_argument('experiment', type=str, choices=['t', 'q', 'w'], help=help_strings['experiment'])
    parser.add_argument('concurrency', type=str, choices=['high', 'low'], help=help_strings['concurrency'])
    parser.add_argument('isolation', type=str, choices=['ru', 'rc', 'rr', 's'], help=help_strings['isolation'])
    parser.add_argument('mpl', type=int, help=help_strings['mpl'])
    parser.add_argument('--config_path', type=str, default='config', help=help_strings['config_path'])
    c_args = parser.parse_args()

    # Create an experiment instance.
    if c_args.database == 'postgres':
        with open(c_args.config_path + '/postgres.json', 'r') as postgres_config_file:
            runner = _PostgresExperimentFactory(
                json.load(postgres_config_file),
                c_args.concurrency
            )(c_args.experiment)

    else:
        with open(c_args.config_path + '/mysql.json', 'r') as mysql_config_file:
            runner = _MySQLExperimentFactory(
                json.load(mysql_config_file),
                c_args.concurrency
            )(c_args.experiment)

    # Run our experiments. Each experiment is a function of MPL.
    with open(c_args.config_path + '/general.json', 'r') as general_config_file:
        general_json = json.load(general_config_file)
    print(f"Running: experiment [{c_args.experiment}], "
          f"{c_args.concurrency} concurrency, "
          f"MPL {c_args.mpl}, "
          f"Isolation {c_args.isolation}")
    runner(c_args.config_path, general_json, c_args.mpl, c_args.isolation)
