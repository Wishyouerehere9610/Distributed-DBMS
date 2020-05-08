Author: Chang Xu

Date: May 4th, 2020

> *May the 4th be with you.*

# Testing scripts of CSE512: Project Phase3

## pj3.py

pj3.py is the main test script of this phase. There are two experiments, one is measuring metrics by changing the size of the dataset, and another is by changing the number of machines. In pj3.py, `task0()` is for the fronter experiment, and `task1()` is for the latter. Both of the two functions call `measure()`, which runs commands for each of the six queries: `["rangequery", "rangejoinquery", "distancequery", "distancejoinquery", "hotzoneanalysis", "hotcellanalysis"]`.

pj3.py uses system argv as input, `argv[1]` represents the choice of which task to do. pj3.py redirects the output to a file named "pj3_log.log", which includes all the metrics such as runtime. 

pj3.py uses `os.system()` to run commands. All the commands are run in new threads, with the main thread measure the metrics, especially memory usage, of the corresponding query. The memory usage is obtained by observing the maximum memory usage of the query process. Runtime and communication cost are measured by using the values after the process subtract the values before the process.

All the file paths, query names and other constant parameters are **HARD CODED** in the code. Please change the corresponding parts in the code if you want to run the code on your machine.

## csvpartitioner.py

csvpartitioner.py serves as an tool function to partition an input file into user specified sizes. 

It takes system argvs as inputs, `argv[1]` is the input file path, and `argv[2]` is the partitioning size. The output file name is determined by these two argvs.