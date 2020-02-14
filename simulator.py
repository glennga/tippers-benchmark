""" This file holds the simulator code, which will execute the transactions. """
from connect import get_mysql_new_connection, get_postgres_new_connection

from typing import Dict
import datetime
import random
import threading
import time
import argparse
import json
import queue
import abc


# Queue of statement sets. Submitted by the workload consumers.
_statement_set_queue = None


class _MySQLConsumerThread(threading.Thread):
    def __init__(self, **kwargs):
        self.conn = get_mysql_new_connection(
            user=kwargs['username'],
            password=kwargs['password'],
            host=kwargs['hostname'],
            database=kwargs['database']
        )
        self.conn.autocommit = False

        self.kwargs = kwargs
        super().__init__(daemon=True)

    def run(self) -> None:
        global _statement_set_queue

        while True:
            statement_set = _statement_set_queue.get()
            _statement_set_queue.task_done()

            # We treat the number 0 as our poison pill here.
            if statement_set == 0:
                break

            # Begin the transaction.
            while True:
                self.conn.start_transaction(isolation_level=self.kwargs['isolation'])
                cur = self.conn.cursor()

                try:
                    for statement in statement_set:
                        cur.execute(statement)

                    break

                except:
                    # If we have an error, wait before retrying.
                    self.conn.rollback()
                    time.sleep(random.random())

            # We have finished our transaction. Commit our work.
            self.conn.commit()


class _PostgresConsumerThread(threading.Thread):
    def __init__(self, **kwargs):
        self.conn = get_postgres_new_connection(
            user=kwargs['username'],
            password=kwargs['password'],
            host=kwargs['hostname'],
            database=kwargs['database']
        )
        self.conn.autocommit = False
        self.conn.isolation_level = kwargs['isolation']

        self.kwargs = kwargs
        super().__init__()

    def run(self) -> None:
        global _statement_set_queue

        while True:
            statement_set = _statement_set_queue.get()
            _statement_set_queue.task_done()

            # We treat the number 0 as our poison pill here.
            if statement_set == 0:
                break

            # Begin the transaction.
            while True:
                cur = self.conn.cursor()

                try:
                    for statement in statement_set:
                        cur.execute(statement)

                    break

                except:
                    # If we have an error, wait before retrying.
                    self.conn.rollback()
                    time.sleep(random.random())

            # We have finished our transaction. Commit our work.
            self.conn.commit()


class _AbstractWorkloadProducer(threading.Thread, abc.ABC):
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        super().__init__()

    @staticmethod
    def _get_table_name(statement: str):
        statement_split_by_into = statement.split("into")
        statement_split_by_values = statement_split_by_into[1].split("values")

        table_name = statement_split_by_values[0]
        table_name = table_name.replace(' ', "")

        return table_name

    def run(self) -> None:
        global _statement_set_queue

        file_handle = open(self.kwargs['filename'], 'r')
        local_query_buffer, local_insert_buffer = {}, {}
        current_timestamp = 0

        print(f'[{datetime.datetime.now()}][simulator.py] Starting to parse file.')
        for i, line in enumerate(file_handle):
            # Parse the query and timestamp.
            record_values = line.strip().split(';')
            statement = record_values[0] + ';'
            timestamp = record_values[1]

            # For our first run, we define a "current" timestamp.
            if i == 0:
                current_timestamp = timestamp

            if timestamp != current_timestamp:
                # If we have reached the next timestamp, this signals to us that we need to flush our buffer.
                for statement_set_tuple in list(local_query_buffer.items()) + list(local_insert_buffer.items()):
                    _statement_set_queue.put(statement_set_tuple[1])

                # Reset our parameters.
                current_timestamp = timestamp
                local_query_buffer.clear()
                local_insert_buffer.clear()

            if "insert" in statement:
                self._aggregate_inserts(statement, local_insert_buffer)
            else:
                self._aggregate_selects(statement, local_query_buffer)

        file_handle.close()
        print(f'[{datetime.datetime.now()}][simulator.py] File is finished being parsed.')

        # Flush the remaining items in our buffer.
        for statement_set_tuple in list(local_query_buffer.items()) + list(local_insert_buffer.items()):
            _statement_set_queue.put(statement_set_tuple[1])

        # Issue the poison pill '0'.
        print(f'[{datetime.datetime.now()}][simulator.py] Issuing poison pill to consumers.')
        for _ in range(self.kwargs['multiprogramming'] + 1):
            _statement_set_queue.put(0)

        print(f'[{datetime.datetime.now()}][simulator.py] Exiting producer thread.')
        exit(0)

    @abc.abstractmethod
    def _aggregate_inserts(self, statement: str, statement_queue: Dict):
        pass

    @abc.abstractmethod
    def _aggregate_selects(self, statement: str, statement_queue: Dict):
        pass


class _InsertOnlyWorkloadProducer(_AbstractWorkloadProducer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _aggregate_inserts(self, statement: str, statement_queue: Dict):
        table_name = self._get_table_name(statement)
        if table_name in statement_queue:
            statement_queue[table_name].append(statement)
        else:
            statement_queue[table_name] = [statement]

    def _aggregate_selects(self, statement: str, statement_queue: Dict):
        pass  # We don't consider select statements.


class _QueryOnlyWorkloadProducer(_AbstractWorkloadProducer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _aggregate_inserts(self, statement: str, statement_queue: Dict):
        pass  # We don't consider insert statements.

    def _aggregate_selects(self, statement: str, statement_queue: Dict):
        statement_queue.update({hash(statement): [statement]})


class _CompleteWorkloadProducer(_AbstractWorkloadProducer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _aggregate_inserts(self, statement: str, statement_queue: Dict):
        table_name = self._get_table_name(statement)
        if table_name in statement_queue:
            statement_queue[table_name].append(statement)
        else:
            statement_queue[table_name] = [statement]

    def _aggregate_selects(self, statement: str, statement_queue: Dict):
        statement_queue.update({hash(statement): [statement]})


def insert_only_workload(**kwargs):
    # Create our shared queue.
    global _statement_set_queue
    _statement_set_queue = queue.Queue(kwargs['multiprogramming'] + 1)

    # Spawn our consumer threads. Wait for them to start.
    for _ in range(kwargs['multiprogramming']):
        _MySQLConsumerThread(**kwargs).start() if kwargs['is_mysql'] else _PostgresConsumerThread(**kwargs).start()
    time.sleep(1)

    # Spawn a producer thread.
    producer_thread = _InsertOnlyWorkloadProducer(**kwargs)
    producer_thread.start()
    producer_thread.join()
    print(f'[{datetime.datetime.now()}][simulator.py] Exiting simulator.')


def query_only_workload(**kwargs):
    # Create our shared queue.
    global _statement_set_queue
    _statement_set_queue = queue.Queue(kwargs['multiprogramming'] + 1)

    # Spawn our consumer threads. Wait for them to start.
    for _ in range(kwargs['multiprogramming']):
        _MySQLConsumerThread(**kwargs).start() if kwargs['is_mysql'] else _PostgresConsumerThread(**kwargs).start()
    time.sleep(1)

    # Spawn a producer thread.
    producer_thread = _QueryOnlyWorkloadProducer(**kwargs)
    producer_thread.start()
    producer_thread.join()
    print(f'[{datetime.datetime.now()}][simulator.py] Exiting simulator.')


def complete_workload(**kwargs):
    # Create our shared queue.
    global _statement_set_queue
    _statement_set_queue = queue.Queue(kwargs['multiprogramming'] + 1)

    # Spawn our consumer threads. Wait for them to start.
    for _ in range(kwargs['multiprogramming']):
        _MySQLConsumerThread(**kwargs).start() if kwargs['is_mysql'] else _PostgresConsumerThread(**kwargs).start()
    time.sleep(1)

    # Spawn a producer thread.
    producer_thread = _CompleteWorkloadProducer(**kwargs)
    producer_thread.start()
    producer_thread.join()
    print(f'[{datetime.datetime.now()}][simulator.py] Exiting simulator.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Simulate transactions using the tipper\'s benchmark.')

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

    with open(c_args.config_path + '/general.json', 'r') as general_config_file:
        general_json = json.load(general_config_file)

    # Define our arguments to the workloads.
    if c_args.database == 'mysql':
        with open(c_args.config_path + '/mysql.json', 'r') as mysql_config_file:
            mysql_json = json.load(mysql_config_file)

        workload_arguments = {
            'filename': general_json[f'data-{c_args.concurrency}-concurrency-workload'],
            'hostname': mysql_json['host'],
            'username': mysql_json['username'],
            'password': mysql_json['password'],
            'database': mysql_json['database'],
            'isolation': {
                'ru': 'READ UNCOMMITTED',
                'rc': 'READ COMMITTED',
                'rr': 'REPEATABLE READ',
                's': 'SERIALIZABLE'
            }[c_args.isolation],
            'multiprogramming': c_args.multiprogramming,
            'is_mysql': True,
        }
    else:
        with open(c_args.config_path + '/postgres.json', 'r') as postgres_config_file:
            postgres_json = json.load(postgres_config_file)

        workload_arguments = {
            'filename': general_json[f'data-{c_args.concurrency}-concurrency-workload'],
            'hostname': postgres_json['host'],
            'username': postgres_json['user'],
            'password': postgres_json['password'],
            'database': postgres_json['database'],
            'isolation': {'ru': 1, 'rc': 2, 'rr': 3, 's': 4}[c_args.isolation],
            'multiprogramming': c_args.multiprogramming,
            'is_mysql': True,
        }

    # Run the experiments.
    print(f'[{datetime.datetime.now()}][simulator.py] Using arguments: {workload_arguments}')
    if c_args.workload == 'i':
        insert_only_workload(**workload_arguments)
    elif c_args.workload == 'q':
        query_only_workload(**workload_arguments)
    else:
        complete_workload(**workload_arguments)
