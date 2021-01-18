# Copyright 2020 The Nadi Data Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import readtextfile
import readjsonfile
import readparquetfile
import writeavrofile
import writetextfile
import writeparquetfile
import utils
import ray
from timeit import default_timer as timer
import time
import pandas as pd

if ray.is_initialized:
    ray.init(address="auto", ignore_reinit_error=True)

''' Initiate Ray tasks from functions'''
'''
@ray.remote
def convert_filter(df, partition_no): 
    df = df.get_partition(partition_no).compute()
    df['Order_Date'] = parsedates.ParseDates(df['Order_Date']).parse_dates_using_lookup()
    df['Ship_Date'] = parsedates.ParseDates(df['Ship_Date']).parse_dates_using_lookup()
    #df = df[df['Region'] == 'Europe']
    start = timer()
    filename = "/Users/sriyan/Downloads/sample_textfile." + str(partition_no) + ".txt"
    df.to_csv(filename, sep='|', header=None, encoding='utf-8')
    print("duration =", timer() - start, " seconds")
    #writeavrofile.WriteAvroFile(df, partition_no, '/Users/sriyan/Downloads/sample_avro_file').write_using_fastavro()
    return 1

def process_incremental(sum, result):
    time.sleep(1) # Replace this with some processing code. 
    return sum + result

start = timer()
result_ids = [convert_filter.remote(df, x) for x in range(df.npartitions)]
sum=0
while len(result_ids):
    done_id, result_ids = ray.wait(result_ids) 
    sum = process_incremental(sum, ray.get(done_id[0])) #passing results to another function as soon as they are available
print("duration =", timer() - start, " seconds\nparallelism=", sum)
'''
''' Initiate Ray actors from class and its methods, each actor will create 1 worker'''
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
        
        start = timer()
        df = df.get_partition(partition_no).compute()
        print("duration =", timer() - start, " seconds for dd to pd")

        df['Order_Date'] = Utils().parse_dates_using_lookup(df['Order_Date'])
        df['Ship_Date'] = Utils().parse_dates_using_lookup(df['Ship_Date'])
        
        
        #df = df[df['Region'] == 'Europe']
        
        start = timer()
        filename = "/tmp/data/sample_textfile." + str(partition_no) + ".txt"
        df.to_csv(filename, sep='|', header=None, encoding='utf-8')
        #writeavrofile.WriteAvroFile(df, partition_no, '/Users/sriyan/Downloads/sample_avro_file').write_using_fastavro()
        print("duration =", timer() - start, " seconds for writing")
        
        return 1


def process_incremental(sum, result):
    time.sleep(1)  # Replace this with some processing code.
    return sum + result


start_pipeline = timer()

start = timer()
'''Register Actors if not registered already'''
flow1_actors = {}
actor_names = ['flow1_actor1','flow1_actor2','flow1_actor3']
for actor_name in actor_names:
    try:
        flow1_actors[actor_name] = ray.get_actor(actor_name)
        print('Actor already registered: {}'.format(actor_name))
    except ValueError:
        flow1_actors[actor_name] = Pipeline.options(name=actor_name, lifetime="detached").remote()
        flow1_actors[actor_name]

print("duration =", timer() - start, " seconds for registering actors")
'''
for actor_name in actor_names:
    flow1_actors[actor_name] = ray.get_actor(actor_name)
'''

df = readtextfile.ReadTextFile(ipfile='/tmp/data/5m_Sales_Records.csv',
                               ipschemafile='/Users/sriyan/Documents/dataprocessor/schema/sample_csv_file.schema',
                               delimiter=',',
                               skiprows=1, 
                               parallel=3).read_using_dask()

result_ids = []
for i,actor_name in enumerate(actor_names):
    result_ids.append(flow1_actors[actor_name].transform.remote(df, i))

sum = 0
while len(result_ids):
    done_id, result_ids = ray.wait(result_ids)
    # passing results to another function as soon as they are available
    sum = process_incremental(sum, ray.get(done_id[0]))

print("duration =", timer() - start, " seconds for Pipeline\nparallelism=", sum)