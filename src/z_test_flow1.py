# Copyright 2020 The Nadi Data Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import dask
import dask.dataframe as dd
import timeit
import readtextfile
import writeparquetfile

t1 = timeit.default_timer()
if __name__ == "__main__":

    df = readtextfile.ReadTextFile(ipfile='/Users/sripri/Downloads/1500000_Sales_Records.csv',
                                   ipschemafile='/Users/sripri/Downloads/schema/sample_csv_file.txt',
                                   delimiter=',', skiprows=1).read_using_dask()

    print(df.divisions)
    df = df.repartition(npartitions=8)
    print(df.divisions)

    writeparquetfile.WriteParquetFile(ipdf=df, opfile="/Users/sripri/Downloads/sample_without_partitions.parquet",partitionkeys=None,
    compression='default', append=False, overwrite=True, write_metadata_file=False, compute=True).write_using_dask()
    writeparquetfile.WriteParquetFile(ipdf=df, opfile="/Users/sripri/Downloads/sample_with_partitions.parquet", partitionkeys=['Region'],
    compression='default', append=False, overwrite=True, write_metadata_file=False, compute=True).write_using_dask()

    df1 = dd.read_parquet(
        "/Users/sripri/Downloads/sample_without_partitions.parquet", engine='pyarrow')
    df2 = dd.read_parquet(
        "/Users/sripri/Downloads/sample_with_partitions.parquet", engine='pyarrow')

    print(df1)
    print(df2)

    print("{} Seconds Needed for read csv".format(timeit.default_timer() - t1))