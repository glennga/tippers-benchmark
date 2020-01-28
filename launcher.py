""" This file is the Python entry point to launch an experiment and observer. """

import argparse

import expr1
import expr2
import expr3

# This value is derived from both MySQL and PostgreSQL.
MAXIMUM_MULTIPROGRAMMING_LEVEL = 8

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run experiments on the tipper\'s benchmark.')

    parser.add_argument('database', type=str, nargs=1, choices=['postgres', 'mysql'],
                        help='Which database to run experiments on.')
    parser.add_argument('experiment', type=str, nargs=1, choices=['throughput', 'query_time', 'workload_time'],
                        help='Which experiment to run.')
    parser.add_argument('--mpl', type=int, nargs='*', choices=list(range(1, MAXIMUM_MULTIPROGRAMMING_LEVEL)),
                        help='Multiprogramming levels to test.')
    args = parser.parse_args()

    if args['database'] == 'postgres':
        pass

    else:
        pass