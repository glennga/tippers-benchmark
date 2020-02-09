from datetime import datetime

file_name_r = "low_concurrency_queries.txt"

file_name_w = "sorted_low_concurrency_queries.txt"

file_r = open(file_name_r, "r")

print("reading")
time_stamp_dict = {}
for line in file_r:
    row = line.rstrip()
    val = row.split(";")

    query = val[0] + ";"
    timestamp = datetime.strptime(val[1], "%Y-%m-%d %H:%M:%S")

    if timestamp in time_stamp_dict:
        time_stamp_dict[timestamp].append(query)

    else:
        time_stamp_dict[timestamp] = [query]

file_r.close()

print("sorting")
vals = [(k, v) for k, v in time_stamp_dict.items()]
vals.sort()

file_w = open(file_name_w, "w")

print("writing")

for value in vals:
    time_stamp = value[0].strftime("%Y-%m-%d %H:%M:%S")
    for query in value[1]:
        file_w.write(query + time_stamp + "\n")

file_w.close()
