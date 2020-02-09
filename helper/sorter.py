from datetime import datetime
from collections import defaultdict


file_name = "semantic_observation_low_concurrency.sql"

file_r = open(file_name, "r")
my_dict = {}

for line in file_r:
    line2 = line.rstrip().split(",")
    if "INSERT" in line2[0]:
        date = line2[-2].strip()
        #print(date, line)
        date_obj = datetime.strptime(date[1:-1], "%Y-%m-%d %H:%M:%S")
        #print(date_obj)
        if date_obj in my_dict:
            my_dict[date_obj].append(line)
        else:
            my_dict[date_obj] = []
        
file_r.close()

#print(my_dict)
vals = [(k,v) for k,v in my_dict.items()]

vals.sort()

file_w = open('sorted_' + file_name, 'w')

for value in vals:
    for j in value[1]:
        file_w.write(j)


file_w.close()
