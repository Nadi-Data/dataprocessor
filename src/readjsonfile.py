# Copyright 2020 The Nadi Data Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import timeit
import dask.dataframe as dd
import pandas as pd

class ReadJaonFile():

    def __init__(self, path, orient="records", lines=None, storage_options=None, 
                 blocksize=None, sample=2 ** 20, encoding="utf-8", errors="strict", 
                 compression="infer", meta=None, engine=pd.read_json):
        
        self.path = path
        self.orient = orient
        self.compression = compression
        self.encoding = encoding
        self.engine = engine

    def read_using_dask(self):
        t1 = timeit.default_timer()
        """ Read json file using dask read_json"""
        ipdf= dd.read_json(self.path, compression=self.compression, encoding=self.encoding)
        print("Time taken : {} seconds for reading json file '{}'".format(timeit.default_timer() - t1, self.path))

        return ipdf