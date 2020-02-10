import random
import threading
from shared import *


# Thread function given a list of queries
def execute_mysql(iso_level, hostname, username, password, database, query_list):
    db = get_mysql_connection(
        user=username,
        password=password,
        host=hostname,
        database=database
    )
    cur = db.cursor()

    try:

        tr_id = random.randint(0, 10000000)
        print("beginning transaction", tr_id)
        db.start_transaction(isolation_level=iso_level)

        for query in query_list:
            cur.execute(query)

        db.commit()
        print("Transaction commited", tr_id)

    except Exception as e:
        print(e)

    finally:
        cur.close()
        db.close()
        print("Finished", tr_id)


def execute_postgres(iso_level, hostname, username, password, database, query_list):
    db = get_postgres_connection(
        user=username,
        password=password,
        host=hostname,
        database=database
    )
    db.autocommit = False
    db.isolation_level = iso_level
    cur = db.cursor()

    try:
        tr_id = random.randint(0, 10000000)
        print("beginning transaction", tr_id)

        for query in query_list:
            cur.execute(query)

        cur.commit()
        print("Transaction commited", tr_id)

    except Exception as e:
        print(e)

    finally:
        cur.close()
        db.close()
        print("Finished", tr_id)


def get_table_name_insert(query):
    query_split_by_into = query.split("INTO")
    query_split_by_values = query_split_by_into[1].split("VALUES")

    table_name = query_split_by_values[0]
    table_name = table_name.replace(' ', "")

    return table_name


def parse_query_list_insert(query_list_i):
    table_query_dict = {}

    for query in query_list_i:
        table_name = get_table_name_insert(query)

        if table_name in table_query_dict:
            table_query_dict[table_name].append(query)

        else:
            table_query_dict[table_name] = [query]

    query_list_return = [v for (k, v) in table_query_dict.items()]
    return query_list_return


def parse_query_list_select(query_list_s):
    query_list_return = []

    for query in query_list_s:
        query_list_return.append([query])

    return query_list_return


def process_transactions(file_name, hostname, username, password, database, is_mysql, iso_level, exp_no):
    file_r = open(file_name, "r")

    query_list_i = []
    query_list_s = []

    cur_timestamp = ""
    ctr = 0
    for line in file_r:
        record = line.strip()
        record_vals = record.split(";")
        query = record_vals[0] + ";"

        timestamp = record_vals[1]

        if ctr == 0:
            cur_timestamp = timestamp

        if timestamp != cur_timestamp:
            query_table_i = parse_query_list_insert(query_list_i)
            query_table_s = parse_query_list_select(query_list_s)

            if exp_no == 1:
                query_table = query_list_i
            elif exp_no == 2:
                query_table = query_list_s
            else:
                query_table = query_table_i + query_table_s

            thread_list = []

            for i in query_table:
                if is_mysql:
                    t = threading.Thread(target=execute_mysql,
                                         args=(iso_level, hostname, username, password, database, i,)
                                         )
                else:
                    t = threading.Thread(target=execute_postgres,
                                         args=(iso_level, hostname, username, password, database, i,)
                                         )
                thread_list.append(t)

            for thread in thread_list:
                thread.start()

            for thread in thread_list:
                thread.join()

            cur_timestamp = timestamp
            query_list_i = []
            query_list_s = []

            if "INSERT" in query and exp_no == 1 or exp_no == 3:
                query_list_i.append(query)

            elif "INSERT" not in query and exp_no == 2 or exp_no == 3:
                query_list_s.append(query)

        else:
            if "INSERT" in query and exp_no == 1 or exp_no == 3:
                query_list_i.append(query)

            elif "INSERT" not in query and exp_no == 2 or exp_no == 3:
                query_list_s.append(query)

        ctr += 1

    file_r.close()



# process_transactions(file_name, hostname, username, password, database)
