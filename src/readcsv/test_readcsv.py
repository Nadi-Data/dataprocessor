# Copyright 2020 The Nadi Data Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import readcsv
import timeit

t1 = timeit.default_timer()
csv_data_generator = readcsv.Readcsv('/Users/sriyan/Downloads/sales_50mb_1500000.csv','r',5000,'\n',',').process_chunks_in_parallel()
print(len(list(csv_data_generator)))
print("{} Seconds Needed for ProcessPoolExecutor".format(timeit.default_timer() - t1))

# 11.09 seconds for print(len(list(csv_data_generator)))