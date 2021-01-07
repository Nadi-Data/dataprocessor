# Copyright 2020 The Nadi Data Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import readtextfile
import readparquetfile
import writeparquetfile
import writetextfile

if __name__ == "__main__":
    """ Read uncompressed text file"""
    df = readtextfile.ReadTextFile(ipfile='/Users/sriyan/Downloads/1500000_Sales_Records.csv',
                                   ipschemafile='schema/sample_csv_file.txt',
                                   delimiter=',', skiprows=1, parallel=4).read_using_dask()

    fwf_df = readtextfile.ReadTextFile(ipfile='/Users/sripri/Downloads/sample_fwf.txt',
                                   ipschemafile='/Users/sripri/Documents/dataprocessor/schema/sample_fwf.types',
                                   delimiter='fixed width', parallel=4).read_using_dask()                                   

    """ Write compressed text file with single_file as True"""
    writetextfile.WriteTextFile(ipdf=df, filename="/Users/sriyan/Downloads/sample_textfile.gz",
                                single_file=True, encoding='utf-8', sep='|', header=False, compression='gzip').write_using_dask()

    """ Read compressed text file"""
    df1 = readtextfile.ReadTextFile(ipfile='/Users/sriyan/Downloads/sample_textfile.gz',
                                    ipschemafile='schema/sample_csv_file.txt',
                                    delimiter='|', skiprows=0, compression='gzip', parallel=4).read_using_dask()

    """ Write Parquet file without partitions ON"""
    writeparquetfile.WriteParquetFile(ipdf=df, opfile="/Users/sriyan/Downloads/sample_without_partitions.parquet", partitionkeys=None,
                                      compression='gzip', engine='auto', append=False, overwrite=True, write_metadata_file=True, compute=True).write_using_dask()

    """ Write Parquet file with partitions ON"""
    writeparquetfile.WriteParquetFile(ipdf=df, opfile="/Users/sriyan/Downloads/sample_with_partitions.parquet", partitionkeys=['Region'],
                                      compression='gzip', engine='auto', append=False, overwrite=True, write_metadata_file=True, compute=True).write_using_dask()

    """ Read Parquet file"""
    df2 = readparquetfile.ReadParquetFile(
        "/Users/sriyan/Downloads/sample_without_partitions.parquet", engine='auto').read_using_dask()
    df3 = readparquetfile.ReadParquetFile(
        "/Users/sriyan/Downloads/sample_with_partitions.parquet", engine='auto').read_using_dask()

    """ Read header from a file"""
    df_header = readtextfile.ReadTextFile(ipfile='/Users/sriyan/Downloads/sample_textfile.txt',
                                          ipschemafile='schema/sample_textfile_header.txt',
                                          delimiter='|').read_using_dask_header()

    """ Read trailer from a file"""
    df_trailer = readtextfile.ReadTextFile(ipfile='/Users/sriyan/Downloads/sample_textfile.txt',
                                           ipschemafile='schema/sample_textfile_trailer.txt',
                                           delimiter='|').read_using_dask_trailer()

    """ Read detail records from a file by skipping header and trailer"""
    df_detail = readtextfile.ReadTextFile(ipfile='/Users/sriyan/Downloads/sample_textfile.txt',
                                          ipschemafile='schema/sample_csv_file.txt',
                                          skiprows=1,
                                          skipfooter=1,
                                          delimiter='|', engine='python').read_using_dask_detail()
    
    print(df_header.head())
    print(df_trailer.head())
    print(df_detail.head())