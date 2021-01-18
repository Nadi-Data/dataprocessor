# Copyright 2020 The Nadi Data Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import readtextfile
import readjsonfile
import readparquetfile
import writeavrofile
import writetextfile
import writeparquetfile
import ray
from timeit import default_timer as timer
import time
import pandas as pd
import dask.dataframe as dd
import uuid

if ray.is_initialized:
    ray.init(address="auto")

df = readtextfile.ReadTextFile(ipfile='/tmp/data/5m_Sales_Records.csv',
                               ipschemafile='/Users/sriyan/Documents/dataprocessor/schema/sample_csv_file.schema',
                               delimiter=',', 
                               skiprows=1, 
                               parallel=6).read_using_dask()

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
        m = num_of_partitions
        n,b,newseq=len(seq),0,[]
        for k in range(m):
            a, b = b, b + (n+k)//m
            newseq.append(seq[a:b])
        return newseq

@ray.remote
class Pipeline():

    def transform(self, df, partition_no): 
        '''All transformations before repartition'''
        start = timer()
        df = df.get_partition(partition_no).compute()
        #df['Order_Date'] = Utils().parse_dates_using_lookup(df['Order_Date'])
        #df['Ship_Date'] = Utils().parse_dates_using_lookup(df['Ship_Date'])
        #df = df[df['Region'] == 'Europe']
        print("duration =", timer() - start, " seconds for transform")
        return df

    def re_partition_sort_data(self, df, partition_metadata, partition_keys, sort_keys, partition_no):
        start = timer()
        '''Prepare partitions based on partition metadata'''
        #df = df[df[partition_keys].isin(partition_metadata)]
        if sort_keys is not None:
            '''Sort data based on sort keys'''
            df.sort_values(by=sort_keys)
        print("duration =", timer() - start, " seconds for preparing partition based on keys")

        start = timer()
        filename = "/tmp/data/sample_textfile." + str(partition_no) + ".txt"
        df.to_csv(filename, sep='|', header=None, encoding='utf-8')
        #writeavrofile.WriteAvroFile(df, partition_no, '/Users/sriyan/Downloads/sample_avro_file').write_using_fastavro()
        print("duration =", timer() - start, " seconds for writing")

if __name__ == "__main__":
    
    start = timer()
    try:
        actors = {}
        result_ids_step1 =[]
        completed_dfs_step1 = []
        completed_ids_step1 = []

        result_ids_step2 = []
        completed_ids_step2 = []

        npartitions = df.npartitions
        parallel = df.npartitions//2

        temp_list = []
        
        ################################STEP1##################################

        '''Workers for executing step1 tasks'''
        for i in range(parallel):
            x = temp_list[-1] if temp_list else 0 
            y = [x, x+1] if x not in temp_list else [x+1, x+2]
            temp_list.extend(y)
            actors[i] = Pipeline.remote()
            result_ids_step1.append(actors[i].transform.remote(df, y[0]))
            result_ids_step1.append(actors[i].transform.remote(df, y[1]))
        
        '''Get step1 status'''
        while len(result_ids_step1):
            done_ids, result_ids_step1 = ray.wait(result_ids_step1)
            completed_ids_step1.extend(done_ids if isinstance(done_ids, list) else [done_ids])
        
        '''Prepare list of data frames to be merged '''
        dflist = [ray.get(objref_id) for objref_id in completed_ids_step1]

        '''Get the merged data frame'''
        df_concat = pd.concat(dflist)
        
        ################################STEP3##################################

        '''Get the partitions metadata based on keys'''
        partition_keys = 'Region'
        sort_keys = ['Order_Date']
        partition_metadata = Utils().define_partitions(seq = list(df_concat[partition_keys].value_counts(dropna=False).keys()), 
                                                            num_of_partitions = parallel)
        print(partition_metadata)

        '''Repartition data: partition using metadata >> sort >> send to workers'''
        for i, x in enumerate(partition_metadata):
            #actors[x] = Pipeline.remote()
            result_ids_step2.append(actors[i].re_partition_sort_data.remote(df_concat[df_concat[partition_keys].isin(x)], x, partition_keys, sort_keys, i))
            #result_ids_step2.append(actors[i].re_partition_sort_data.remote(df_concat, x, partition_keys, sort_keys, i))

        '''Get step2 status'''
        while len(result_ids_step2):
            done_ids, result_ids_step2 = ray.wait(result_ids_step2)
            completed_ids_step2.extend(done_ids if isinstance(done_ids, list) else [done_ids])

    finally:
        pass
        '''for x in actors.keys():
            ray.kill(actors[x])'''

    print("duration =", timer() - start, " seconds")