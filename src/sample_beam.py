import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from csv import reader
from collections import namedtuple
import typing
import logging
import timeit
import dask.dataframe as dd
import readtextfile
import writetextfile

pipeline_options = PipelineOptions(['--direct_num_workers', '6'], ['--direct_running_mode', 'multi_threading'])
csv_file_current= '/Users/sripri/Downloads/1500000_Sales_Records.csv'

class IpSchema(typing.NamedTuple):
    Region: str
    Country: str
    Item_Type: str
    Sales_Channel: str
    Order_Priority: str
    Order_Date: str
    Order_ID: int
    Ship_Date: str
    Units_Sold: int
    Unit_Price: float
    Unit_Cost: float
    Total_Revenue: float
    Total_Cost: float
    Total_Profit: float

def read_headers(csv_file):
    with open(csv_file, 'r') as f:
        header_line = f.readline().strip().replace(' ', '_')
    return next(reader([header_line]))

current_data_headers = read_headers(csv_file_current)
UsCovidData = namedtuple('UsCovidData', current_data_headers)

class UsCovidDataCsvReader(beam.DoFn):
  def __init__(self, schema):
    self._schema = schema
    
  def process(self, element):
    values = [int(val) if val.isdigit() else val for val in next(reader([element]))]
    return [self._schema(*values)]

def read_write_csv(IpSchema, pipeline_options):
    with beam.Pipeline(options=pipeline_options) as p:
        csv_data = p | 'Create PCollection from files' >> beam.io.ReadFromText(csv_file_current, skip_header_lines=1)
        current_data = csv_data | 'Parse' >> beam.ParDo(UsCovidDataCsvReader(UsCovidData))
        (current_data | 'Filter data' >> beam.Filter(lambda row : row.Region == 'Europe')
                      | 'NamedTuple to tuple' >> beam.Map(lambda row : tuple(row))
                      | 'Write text file' >> beam.io.WriteToText('/Users/sripri/Downloads/test', file_name_suffix='.txt').with_output_types(IpSchema))

if __name__ == '__main__':
    t1 = timeit.default_timer()
    #logging.getLogger().setLevel(logging.INFO)
    read_write_csv(IpSchema, pipeline_options)
    print(timeit.default_timer() - t1)

    t2 = timeit.default_timer()
    df = readtextfile.ReadTextFile(ipfile=csv_file_current,
                                   ipschemafile='schema/sample_csv_file.txt',
                                   delimiter=',', skiprows=1, parallel=6).read_using_dask()
    df = df[df['Region'] == 'Europe']
    writetextfile.WriteTextFile(ipdf=df, filename="/Users/sripri/Downloads/test.txt",
                                single_file=False, encoding='utf-8', sep=',', header=False).write_using_dask()
    print(timeit.default_timer() - t2)

