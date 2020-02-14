""" This file is the Python entry point to launch an experiment and observer. """
from simulator import insert_only_workload, query_only_workload, complete_workload
from destructor import teardown_mysql, teardown_postgres

from typing import Callable, Dict
import datetime
import argparse
import json
import abc


class _GenericWorkloadFactory(abc.ABC):
    @abc.abstractmethod
    def _insert_only_workload(self, isolation: str, mpl: int, _general_json: Dict[str, str], config_path: str):
        pass

    @abc.abstractmethod
    def _query_only_workload(self, isolation: str, mpl: int, _general_json: Dict[str, str], config_path: str):
        pass

    @abc.abstractmethod
    def _complete_workload(self, isolation: str, mpl: int, _general_json: Dict[str, str], config_path: str):
        pass

    def __call__(self, workload: str) -> Callable:
        if workload == 'i':
            return self._insert_only_workload
        elif workload == 'q':
            return self._query_only_workload
        else:
            return self._complete_workload


class _PostgresWorkloadFactory(_GenericWorkloadFactory):
    def __init__(self, postgres_json, concurrency: str):
        self.postgres_json = postgres_json
        self.concurrency = concurrency

    def _generate_workload_arguments(self, isolation: str, mpl: int, _general_json: Dict[str, str], config_path: str):
        return {
            'filename': _general_json[f'data-{self.concurrency}-concurrency-workload'],
            'hostname': self.postgres_json['host'],
            'username': self.postgres_json['user'],
            'password': self.postgres_json['password'],
            'database': self.postgres_json['database'],
            'isolation': {'ru': 1, 'rc': 2, 'rr': 3, 's': 4}[isolation],
            'multiprogramming': mpl,
            'max_retries': int(_general_json['max-retries']),
            'is_mysql': False,
        }

    def _insert_only_workload(self, isolation: str, mpl: int, _general_json: Dict[str, str], config_path: str):
        insert_only_workload(**self._generate_workload_arguments(isolation, mpl, _general_json, config_path))

    def _query_only_workload(self, isolation: str, mpl: int, _general_json: Dict[str, str], config_path: str):
        query_only_workload(**self._generate_workload_arguments(isolation, mpl, _general_json, config_path))

    def _complete_workload(self, isolation: str, mpl: int, _general_json: Dict[str, str], config_path: str):
        complete_workload(**self._generate_workload_arguments(isolation, mpl, _general_json, config_path))


class _MySQLWorkloadFactory(_GenericWorkloadFactory):
    def __init__(self, mysql_json, concurrency: str):
        self.mysql_json = mysql_json
        self.concurrency = concurrency

    def _generate_workload_arguments(self, isolation: str, mpl: int, _general_json: Dict[str, str], config_path: str):
        return {
            'filename': _general_json[f'data-{self.concurrency}-concurrency-workload'],
            'hostname': self.mysql_json['host'],
            'username': self.mysql_json['username'],
            'password': self.mysql_json['password'],
            'database': self.mysql_json['database'],
            'isolation': {
                'ru': 'READ UNCOMMITTED',
                'rc': 'READ COMMITTED',
                'rr': 'REPEATABLE READ',
                's': 'SERIALIZABLE'
            }[isolation],
            'multiprogramming': mpl,
            'max_retries': int(_general_json['max-retries']),
            'is_mysql': True,
        }

    def _insert_only_workload(self, isolation: str, mpl: int, _general_json: Dict[str, str], config_path):
        insert_only_workload(**self._generate_workload_arguments(isolation, mpl, _general_json, config_path))

    def _query_only_workload(self, isolation: str, mpl: int, _general_json: Dict[str, str], config_path):
        query_only_workload(**self._generate_workload_arguments(isolation, mpl, _general_json, config_path))

    def _complete_workload(self, isolation: str, mpl: int, _general_json: Dict[str, str], config_path):
        complete_workload(**self._generate_workload_arguments(isolation, mpl, _general_json, config_path))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run experiments on the tipper\'s benchmark.')

    help_strings = {
        "database": 'Which database to run experiments on.',
        "workload": "Which workload to run. i=insert-only, q=query-only, c=complete.",
        "concurrency": 'Type of concurrency experiment to run.',
        "isolation": "Isolation level to run.",
        "multiprogramming": 'Multiprogramming level to run.',
        "config_path": 'Location of configuration files.'
    }
    parser.add_argument('database', type=str, choices=['postgres', 'mysql'], help=help_strings['database'])
    parser.add_argument('workload', type=str, choices=['i', 'q', 'c'], help=help_strings['workload'])
    parser.add_argument('concurrency', type=str, choices=['high', 'low'], help=help_strings['concurrency'])
    parser.add_argument('isolation', type=str, choices=['ru', 'rc', 'rr', 's'], help=help_strings['isolation'])
    parser.add_argument('multiprogramming', type=int, help=help_strings['multiprogramming'])
    parser.add_argument('--config_path', type=str, default='config', help=help_strings['config_path'])
    c_args = parser.parse_args()

    # Create an experiment instance.
    if c_args.database == 'postgres':
        with open(c_args.config_path + '/postgres.json', 'r') as postgres_config_file:
            runner = _PostgresWorkloadFactory(
                json.load(postgres_config_file),
                c_args.concurrency
            )(c_args.workload)

    else:
        with open(c_args.config_path + '/mysql.json', 'r') as mysql_config_file:
            runner = _MySQLWorkloadFactory(
                json.load(mysql_config_file),
                c_args.concurrency
            )(c_args.workload)

    # Run our workload. Each experiment is a function of MPL.
    with open(c_args.config_path + '/general.json', 'r') as general_config_file:
        general_json = json.load(general_config_file)
    print(f"[{datetime.datetime.now()}][runner.py] Workload ({c_args.workload}), "
          f"Concurrency ({c_args.concurrency}), "
          f"MPL ({c_args.multiprogramming}), "
          f"Isolation ({c_args.isolation})")
    runner(c_args.isolation, c_args.multiprogramming, general_json, c_args.config_path)
