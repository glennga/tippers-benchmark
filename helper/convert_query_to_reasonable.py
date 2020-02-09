file_name_r = "queries.txt"
file_name_w = "queries_reasonable.txt"

file_r = open(file_name_r, "r")

quote_ctr = 0
query_list = []
query = ""
timestamp = ""
for line in file_r:
    row = line.rstrip()
    if row[-1] == '"':
        quote_ctr += 1

        if quote_ctr % 2 == 0:
            query_list.append((query, timestamp))
            print(query)
            #print(query, timestamp)
            quote_ctr = 0
            timestamp = ""
            query = ""

        else:
            timestamp = row[0:-2]
            timestamp = timestamp.replace("T", " ")
            timestamp = timestamp.replace("Z", "")
            #print(timestamp)

    else:
        query += row + " "



    
    #break


file_r.close()






file_w = open(file_name_w, "w")

for query in query_list:
    file_w.write(query[0] + ";" + query[1] + "\n")

file_w.close()
