""" This file holds the simulator code, which will execute the transactions. """
from connect import get_mysql_new_connection, get_postgres_new_connection

import datetime
import random
import threading
import time
import argparse
import json
import queue
import abc


# Queue of query sets. Submitted by the workload consumers.
_QUEUE_MAXIMUM_SIZE = 1000  # Hard-coded max right now...
_query_set_queue = queue.Queue(_QUEUE_MAXIMUM_SIZE)


class _MySQLConsumerThread(threading.Thread):
    def __init__(self, **kwargs):
        self.conn = get_mysql_new_connection(
            user=kwargs['username'],
            password=kwargs['password'],
            host=kwargs['hostname'],
            database=kwargs['database']
        )
        self.kwargs = kwargs
        super().__init__()

    def run(self) -> None:
        global _query_set_queue

        while True:
            query_set = _query_set_queue.get()
            _query_set_queue.task_done()

            # Begin the transaction.
            start_of_transaction = datetime.datetime.now()
            transaction_id = random.randint(0, 10000000)
            # print(f'Transaction {transaction_id} started at {start_of_transaction}.')
            self.conn.start_transaction(isolation_level=self.kwargs['isolation'])

            cur = self.conn.cursor()
            for query in query_set:
                # Keep track of how often we have to retry a statement.
                retry_count = 0

                while retry_count < self.kwargs['max_retries']:
                    try:
                        cur.execute(query)
                        break

                    except:
                        retry_count += 1  # If we have an error, wait before retrying.
                        time.sleep(random.random())

            # We have finished our transaction. Commit our work.
            end_of_transaction = datetime.datetime.now()
            # print(f'Transaction {transaction_id} ended at {end_of_transaction}.')
            self.kwargs['observer'].record_observation(start_of_transaction, end_of_transaction)
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
        global _query_set_queue

        while True:
            query_set = _query_set_queue.get()
            _query_set_queue.task_done()

            # Begin the transaction.
            start_of_transaction = datetime.datetime.now()
            transaction_id = random.randint(0, 10000000)
            print(f'Transaction {transaction_id} started at {start_of_transaction}.')

            cur = self.conn.cursor()
            for query in query_set:
                # Keep track of how often we have to retry a statement.
                retry_count = 0

                while retry_count < self.kwargs['max_retries']:
                    try:
                        cur.execute(query)
                        break

                    except:
                        retry_count += 1  # If we have an error, wait before retrying.
                        time.sleep(random.random())

            # We have finished our transaction. Commit our work.
            end_of_transaction = datetime.datetime.now()
            print(f'Transaction {transaction_id} ended at {end_of_transaction}.')
            self.kwargs['observer'].record_observation(start_of_transaction, end_of_transaction)
            self.conn.commit()


class _AbstractWorkloadProducer(threading.Thread, abc.ABC):
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        super().__init__()

    def run(self) -> None:
        global _QUEUE_MAXIMUM_SIZE, _query_set_queue

        file_handle = open(self.kwargs['filename'], 'r')
        local_query_buffer, current_timestamp = [], 0

        for i, line in enumerate(file_handle):
            # Parse the query and timestamp.
            record_values = line.strip().split(';')
            parsed_query = record_values[0] + ';'
            timestamp = record_values[1]

            # For our first run, we define a "current" timestamp.
            if i == 0:
                current_timestamp = timestamp

            if timestamp != current_timestamp:
                # If we have reached the next timestamp, this signals to us that we need to flush our buffer.
                for query in local_query_buffer:
                    _query_set_queue.put([query])

                # Reset our parameters.
                current_timestamp = timestamp
                local_query_buffer.clear()

            if self._filter_query(parsed_query):
                local_query_buffer.append(parsed_query)

        file_handle.close()
        self.kwargs['observer'].end_logging()

    @abc.abstractmethod
    def _filter_query(self, query):
        pass


class _InsertOnlyWorkloadProducer(_AbstractWorkloadProducer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _filter_query(self, query):
        return "INSERT" in query


class _QueryOnlyWorkloadProducer(_AbstractWorkloadProducer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _filter_query(self, query):
        return "INSERT" not in query


class _CompleteWorkloadProducer(_AbstractWorkloadProducer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _filter_query(self, query):
        return True


def insert_only_workload(**kwargs):
    # Spawn our consumer threads. Wait for them to start.
    for _ in range(kwargs['multiprogramming']):
        _MySQLConsumerThread(**kwargs).start() if kwargs['is_mysql'] else _PostgresConsumerThread().start()
    time.sleep(1)

    # Spawn a producer thread.
    _InsertOnlyWorkloadProducer(**kwargs).start()


def query_only_workload(**kwargs):
    # Spawn our consumer threads. Wait for them to start.
    for _ in range(kwargs['multiprogramming']):
        _MySQLConsumerThread(**kwargs).start() if kwargs['is_mysql'] else _PostgresConsumerThread().start()
    time.sleep(1)

    # Spawn a producer thread.
    _QueryOnlyWorkloadProducer(**kwargs).start()


def complete_workload(**kwargs):
    # Spawn our consumer threads. Wait for them to start.
    for _ in range(kwargs['multiprogramming']):
        _MySQLConsumerThread(**kwargs).start() if kwargs['is_mysql'] else _PostgresConsumerThread().start()
    time.sleep(1)

    # Spawn a producer thread.
    _CompleteWorkloadProducer(**kwargs).start()


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

    # Define a dummy observer.
    class _DummyObserver:
        def record_observation(self, start_of_transaction, end_of_transaction):
            pass

        def end_logging(self):
            pass

    # Define our arguments to the workloads. We define a dummy observer here.
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
            'max_retries': int(general_json['max-retries']),
            'observer': _DummyObserver(),
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
            'isolation': {'ru': 0, 'rc': 1, 'rr': 2, 's': 3}[c_args.isolation],
            'multiprogramming': c_args.multiprogramming,
            'max_retries': int(general_json['max-retries']),
            'observer': _DummyObserver(),
            'is_mysql': True,
        }

    # Run the experiments.
    print(f'Using arguments: {workload_arguments}')
    if c_args.workload == 'i':
        insert_only_workload(**workload_arguments)
    elif c_args.workload == 'q':
        query_only_workload(**workload_arguments)
    else:
        complete_workload(**workload_arguments)
