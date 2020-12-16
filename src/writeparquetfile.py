# Copyright 2020 The Nadi Data Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import timeit
import dask.dataframe as dd

class WriteParquetFile():

    def __init__(self, ipdf, opfile, partitionkeys=None, compression='default', engine='auto', append=False, overwrite=False, write_metadata_file=False, compute=True):
        
        self.ipdf = ipdf
        self.opfile = opfile
        self.partitionkeys = partitionkeys
        self.compression = compression
        self.engine = engine
        self.append = append
        self.overwrite = overwrite
        self.write_metadata_file = write_metadata_file
        self.compute = compute

    def write_using_dask(self):
        t1 = timeit.default_timer()
        """ write parquet file using dask to_parquet"""
        self.ipdf.to_parquet(self.opfile, engine=self.engine, 
                             partition_on=self.partitionkeys,
                             compression = self.compression,
                             append = self.append,
                             overwrite = self.overwrite,
                             write_metadata_file = self.write_metadata_file,
                             compute = self.compute)
        print("Time taken : {} seconds for writing parquet file '{}'".format(timeit.default_timer() - t1, self.opfile))
