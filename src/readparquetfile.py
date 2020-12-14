# Copyright 2020 The Nadi Data Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import timeit
import dask.dataframe as dd

class ReadParquetFile():

    def __init__(self, path, columns=None, index=None, engine='auto'):
        
        self.path = path
        self.columns = columns
        self.index = index
        self.engine = engine

    def read_using_dask(self):
        t1 = timeit.default_timer()
        """ write parquet file using dask to_parquet"""
        ipdf= dd.read_parquet(self.path, engine=self.engine)
        print("Time taken : {} for reading parquet file '{}'".format(timeit.default_timer() - t1, self.path))

        return ipdf
