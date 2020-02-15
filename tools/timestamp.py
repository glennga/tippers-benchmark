from datetime import datetime

file_name = "results/launcher-postgres.log"
file_r = open(file_name, "r")
timestamps = []

start_timestamp = ""
end_timestamp = ""

workload = ""
concurrency = ""
mpl = ""
isolation = ""

for line in file_r:
    if "Workload" in line:
        varbs = line.rstrip().split("]")
        varbs_2 = varbs[-1].split(",")

        workload = varbs_2[0].split("(")[1][0:-1]
        concurrency = varbs_2[1].split("(")[1][0:-1]
        mpl = varbs_2[2].split("(")[1][0:-1]
        isolation = varbs_2[3].split("(")[1][0:-1]


    
    elif "Starting to parse file" in line:
        start_timestamp = line.split("]")[0][1:]


    elif "Exiting simulator" in line:
        end_timestamp = line.split("]")[0][1:]

        timestamps.append((start_timestamp, end_timestamp, workload, concurrency, mpl, isolation))

file_r.close()

file_name_2 = "results/postgres-log.csv"
#print(timestamps)
file_w = open(file_name_2, "w")

serial_ctr = 1
for time in timestamps:

    start_time = datetime.strptime(time[0], "%Y-%m-%d %H:%M:%S.%f")    
    end_time = datetime.strptime(time[1], "%Y-%m-%d %H:%M:%S.%f")

    time_diff = end_time - start_time
    time_diff_str = str(time_diff.total_seconds())
    

    
    file_w.write(str(serial_ctr) + "," + time[0] + "," + time[1] + "," + time_diff_str+ "," + time[2] + ","+ time[3] + "," + time[4] + "," + time[5] + "\n")


    serial_ctr += 1

file_w.close()
