# Copyright 2020 The Nadi Data Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import timeit
import pandas as pd

class Utils():

    def __init__(self):
        pass

    def parse_dates_using_lookup(self, date_column, format_to='%Y%m%d', infer_datetime_format=False):
        """
        This is an extremely fast approach to datetime parsing.
        For large data, the same dates are often repeated. Rather than
        re-parse these, we store all unique dates, parse them, and
        use a lookup to convert all dates.
        """
        #dates = {date:pd.to_datetime(date, format=self.format_to) for date in self.date_column.unique()}
        dates = {date:pd.to_datetime(date, infer_datetime_format=infer_datetime_format) for date in date_column.unique()}
        return date_column.map(dates)

    def define_partitions(self, seq, num_of_partitions):
        """
        This needs to be modified to redice skew
        """
        
        m =4
        n,b,newseq=len(seq),0,[]
        for k in range(m):
            a, b = b, b + (n+k)//m
            newseq.append(seq[a:b])
        return newseq