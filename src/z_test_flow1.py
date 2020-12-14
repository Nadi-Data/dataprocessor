# Copyright 2020 The Nadi Data Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import readtextfile
import readparquetfile
import writeparquetfile
import writetextfile

if __name__ == "__main__":
    """ Read uncompressed text file"""
    df = readtextfile.ReadTextFile(ipfile='/Users/sripri/Downloads/1500000_Sales_Records.csv',
                                   ipschemafile='/Users/sripri/Downloads/schema/sample_csv_file.txt',
                                   delimiter=',', skiprows=1,parallel=4).read_using_dask()
    
    """ Write compressed text file with single_file as True"""
    writetextfile.WriteTextFile(ipdf=df, filename="/Users/sripri/Downloads/sample_textfile.gzip", 
    single_file=True, encoding='utf-8', sep='|',header=False, mode='wt', compression='gzip', compute=True).write_using_dask()
    
    """ Read compressed text file"""
    df1 = readtextfile.ReadTextFile(ipfile='/Users/sripri/Downloads/sample_textfile.gzip',
                                   ipschemafile='/Users/sripri/Downloads/schema/sample_csv_file.txt',
                                   delimiter='|', skiprows=0, compression='gzip', parallel=4).read_using_dask()
    """ Write Parquet file without partitions ON"""
    writeparquetfile.WriteParquetFile(ipdf=df, opfile="/Users/sripri/Downloads/sample_without_partitions.parquet",partitionkeys=None,
    compression='gzip',engine='auto', append=False, overwrite=True, write_metadata_file=True, compute=True).write_using_dask()

    """ Write Parquet file with partitions ON"""
    writeparquetfile.WriteParquetFile(ipdf=df, opfile="/Users/sripri/Downloads/sample_with_partitions.parquet", partitionkeys=['Region'],
    compression='gzip',engine='auto', append=False, overwrite=True, write_metadata_file=True, compute=True).write_using_dask()
    
    """ Read Parquet file"""
    df2 = readparquetfile.ReadParquetFile("/Users/sripri/Downloads/sample_without_partitions.parquet", engine='auto').read_using_dask()
    df3 = readparquetfile.ReadParquetFile("/Users/sripri/Downloads/sample_with_partitions.parquet", engine='auto').read_using_dask()