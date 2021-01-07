# Copyright 2020 The Nadi Data Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import timeit
import dask.dataframe as dd

class ReadOrcFile():

    def __init__(self, path, columns=None):
        
        self.path = path
        self.columns = columns

    def read_using_dask(self):
        t1 = timeit.default_timer()
        """ write parquet file using dask to_parquet"""
        ipdf= dd.read_orc(self.path, columns=self.columns)
        print("Time taken : {} seconds for reading parquet file '{}'".format(timeit.default_timer() - t1, self.path))

        return ipdf
