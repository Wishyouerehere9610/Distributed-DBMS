# Copyright 2020 by Chang Xu
# May the 4th be with you.

import psutil as ps
import os
import sys
import time
import threading

cmd1 = "~/hadoop/spark-2.3.2-bin-hadoop2.7/bin/spark-submit ~/hadoop/spark-2.3.2-bin-hadoop2.7/CSE512-Project-Phase1-Template-assembly-final.jar ~/hadoop/spark-2.3.2-bin-hadoop2.7/result/output {0} ~/hadoop/spark-2.3.2-bin-hadoop2.7/src/resources/{1} {2}"
cmd2 = "~/hadoop/spark-2.3.2-bin-hadoop2.7/bin/spark-submit ~/PJ2-XC/target/scala-2.11/CSE512-Hotspot-Analysis-Template-assembly-0.1.0.jar ~/PJ2-XC/test/output {0}  ~/PJ2-XC/src/resources/{1} {2}"
pj1_file_path = "~/hadoop/spark-2.3.2-bin-hadoop2.7/src/resources/"
range_query_paras = "-93.63173,33.0183,-93.359203,33.219456"
dist_query_paras = "-88.331492,32.324142 1"
zone_hotzone = "~/PJ2-XC/src/resources/zone-hotzone.csv"

# cmd1 = cmd1.format("rangeQuery", "1000.csv")
# print(cmd1)

functions = [
    "rangequery",
    "rangejoinquery",
    "distancequery",
    "distancejoinquery",
    "hotzoneanalysis",
    "hotcellanalysis"
]

datasets = [
    "arealm10000.csv", "part_2_arealm10000.csv", "part_4_arealm10000.csv",
    "zcta10000.csv", "part_2_zcta10000.csv", "part_4_zcta10000.csv",
    "point-hotzone.csv", "part_2_point-hotzone.csv", "part_4_point-hotzone.csv",
    "yellow_trip_sample_100000.csv", "part_2_yellow_trip_sample_100000.csv", "part_4_yellow_trip_sample_100000.csv"
]

cmd_finish_flag = False
cmd_time = 0

def run_cmd(cmd):
    start_time = time.time()
    os.system(cmd)
    end_time = time.time()
    global cmd_time
    global cmd_finish_flag
    cmd_time = end_time-start_time
    cmd_finish_flag = True

def measure(f0, f1_1, f1_2, f2, f3_1, f3_2, f4, f5):
    global cmd_time
    global cmd_finish_flag
    for i in range(6):
        cmd = ""
        mem = 0
        func = functions[i]
        cmd_finish_flag = False
        if i == 0:
            para = range_query_paras
            cmd = cmd1.format(func, f0, para)
        elif i == 1:
            para = pj1_file_path + f1_2
            cmd = cmd1.format(func, f1_1, para)
        elif i == 2:
            para = dist_query_paras
            cmd = cmd1.format(func, f2, para)
        elif i == 3:
            para = pj1_file_path + f3_2 + " " + "0.1"
            cmd = cmd1.format(func, f3_1, para)
        elif i == 4:
            para = zone_hotzone
            cmd = cmd2.format(func, f4, para)
        else:
            para = ""
            cmd = cmd2.format(func, f5, para)
        print(cmd)
        # start_mem = ps.virtual_memory()[3] #used
        
        cmd_thread = threading.Thread(target=run_cmd, args=(cmd,))
        cmd_thread.start()
        old_mem = ps.virtual_memory()[3]
        mem = old_mem
        old_network = ps.net_io_counters(pernic=True)["eth0"]
        old_out = old_network[0]
        old_in = old_network[1]
        while not cmd_finish_flag:
            mem = max(mem, ps.virtual_memory()[3])
            # ps.net_io_counters()
            time.sleep(0.01)
        # cmd_thread.join()
        # end_mem = ps.virtual_memory()[3] #used
        network = ps.net_io_counters(pernic=True)["eth0"]
        net_out = round((network[0]-old_out)/1024, 2)
        net_in = round((network[1]-old_in)/1024, 2)
        # ps.net_io_counters.cache_clear()
        mem -= old_mem
        run_time = round(cmd_time, 2)
        # mem = end_mem - start_mem
        print("Run time {0}s, Mem usage: {1}MB, Network Out: {2}KB, Network In: {3}KB".format(
            run_time, (round(mem/1024/1024, 2)), net_out, net_in)
        )

def task0(): #Change the size of datasets
    sizes = ["large", "medium", "small"]
    for i, size in enumerate(sizes):
        print("------dataset size: {}------".format(size))
        measure(f0=datasets[i], f1_1=datasets[i], f1_2=datasets[i+3], f2=datasets[i], 
            f3_1=datasets[i], f3_2=datasets[i], f4=datasets[i+6], f5=datasets[i+9])

def task1(): #Change the num of machines:
    i = 1 #medium
    measure(f0=datasets[i], f1_1=datasets[i], f1_2=datasets[i+3], f2=datasets[i], 
            f3_1=datasets[i], f3_2=datasets[i], f4=datasets[i+6], f5=datasets[i+9])

if __name__ == "__main__":
    choice = int(sys.argv[1])
    sys.stdout = open("pj3_log.log", "a")
    if choice == 0:
        print("Single machine; change the size of dataset")
        task0()
    elif choice == 1:
        print("Num of machines: 2; size of dataset: Medium")
        task1()
    else:
        print("invalid choice: must between 0 and 1.")
        exit()  
    print("------------------------------------------")

