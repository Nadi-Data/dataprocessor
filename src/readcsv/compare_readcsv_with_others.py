# Copyright 2020 The Nadi Data Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import readcsv
import timeit
import csv
import pandas as pd
import os


t1 = timeit.default_timer()
csv_data = readcsv.Readcsv('/Users/sriyan/Downloads/sales_50mb_1500000.csv','r',5000,'\n',',').process_chunks_in_parallel()
#print(len(list(csv_data)))
print("{} Seconds Needed for ProcessPoolExecutor".format(timeit.default_timer() - t1))

# 11.09 seconds for print(len(list(csv_data)))

t2 = timeit.default_timer()
csv_data = pd.read_csv('/Users/sriyan/Downloads/sales_50mb_1500000.csv')
#print(len(csv_data))
print("{} Seconds Needed for pandas".format(timeit.default_timer() - t2))

# 2.58 seconds for print(len(csv_data))

t3 = timeit.default_timer()
with open('/Users/sriyan/Downloads/sales_50mb_1500000.csv','r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
#print(len(rows))
print("{} Seconds Needed for csv".format(timeit.default_timer() - t3))  

# 21 seconds for print(len(rows))