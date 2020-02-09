import mysql.connector
import random
import threading



#Thread function given a list of queries
def execute_query(hostname, username, password, database, query_list):


    db = mysql.connector.connect(
        host = hostname,
        user = username, 
        passwd = password,
        database = database

        )

    conn = db.cursor()

    try:
        
        tr_id = random.randint(0,10000000)
        print("beginning transaction", tr_id)
        db.start_transaction()
    
        for query in query_list:
            conn.execute(query)

        db.commit()
        print("Transaction commited", tr_id)

    except Exception as e:
        print(e)

    finally:
        conn.close()
        db.close()
        print("Finished", tr_id)

        



def get_table_name_insert(query):
    query_split_by_into = query.split("INTO")
    query_split_by_values = query_split_by_into[1].split("VALUES")

    table_name = query_split_by_values[0]
    table_name = table_name.replace(' ', "")
    
    return table_name







def parse_query_list_insert(query_list_i):
    query_list_return = []

    table_query_dict = {}

    for query in query_list_i:
        table_name = get_table_name_insert(query)

        if table_name in table_query_dict:
            table_query_dict[table_name].append(query)

        else:
            table_query_dict[table_name] = [query]


    query_list_return = [v for (k,v) in table_query_dict.items()]
    return query_list_return

def parse_query_list_select(query_list_s):
    query_list_return = []

    for query in query_list_s:
        query_list_return.append([query])


    return query_list_return






def process_transactions(file_name, hostname, username, password, database):
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

            query_table = query_table_i + query_table_s

            thread_list = []
            
            
            for i in query_table:
                t = threading.Thread(target=execute_query, args = (hostname, username, password, database, i,))
                thread_list.append(t)

            for thread in thread_list:
                thread.start()

            for thread in thread_list:
                thread.join()



            cur_timestamp = timestamp
            query_list_i = []
            query_list_s = []

            if "INSERT" in query:
                query_list_i.append(query)

            else:
                query_list_s.append(query)



        else:
            if "INSERT" in query:
                query_list_i.append(query)

            else:
                query_list_s.append(query)


        ctr += 1 




    file_r.close()



#if __name__ == '__main__':

    

    #process_transactions(file_name, hostname, username, password, database)
    
    