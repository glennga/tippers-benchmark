file_name_r = "sorted_semantic_observation_low_concurrency.sql"

file_name_w = "semantic_observation_lc.txt"

file_r = open(file_name_r, "r")
file_w = open(file_name_w, "w")

for line in file_r:
    line = line.rstrip()
    time_stamp = line.split(",")[-2]
    time_stamp = time_stamp.replace("'", "")
    time_stamp = time_stamp[1:]
    # time_stamp = time_stamp.replace(" ", "")

    file_w.write(line + time_stamp + "\n")

file_r.close()
file_w.close()
