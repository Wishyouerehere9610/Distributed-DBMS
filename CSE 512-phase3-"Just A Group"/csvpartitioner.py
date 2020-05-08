import csv
import sys

fname = sys.argv[1]
p = int(sys.argv[2]) # partition size
out = "part_"+str(p)+"_"+fname

f_in = open(fname, 'r')
f_out = open(out, 'w')

csvreader = csv.reader(f_in)
csvwriter = csv.writer(f_out)

counter = 0
for row in csvreader:
    if counter % p == 0:
        csvwriter.writerow(row)
    counter += 1

f_in.close()
f_out.close()