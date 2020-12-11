# Copyright 2020 The Nadi Data Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import dask
import dask.dataframe as dd
import timeit

if __name__ == "__main__":
    
    t1 = timeit.default_timer()
    df = dd.read_csv("/Users/sripri/Downloads/1500000_Sales_Records.csv")
    #print(df)
    df.head(500000)
    #df.to_parquet("/Users/sripri/Downloads/1500000_Sales_Records.parquet", engine='pyarrow')
    print("{} Seconds Needed for csv".format(timeit.default_timer() - t1))

    t2 = timeit.default_timer()
    df1 = dd.read_parquet("/Users/sripri/Downloads/1500000_Sales_Records.parquet", engine='pyarrow')
    df1.head(500000)
    print("{} Seconds Needed for Parquet".format(timeit.default_timer() - t2))