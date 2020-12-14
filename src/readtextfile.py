# Copyright 2020 The Nadi Data Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import timeit
import dask.dataframe as dd

class ReadTextFile():
    
    def __init__(self, ipfile, ipschemafile, delimiter, skiprows=0, header=None, compression=None ,index_col=False,blocksize=None, parallel=4):
        self.ipfile = ipfile
        self.ipschemafile = ipschemafile
        self.sep = delimiter
        self.skiprows = skiprows
        self.header = header
        self.compression = compression
        self.blocksize = blocksize
        self.parallel = parallel

    def read_using_dask(self):
        t1 = timeit.default_timer()
        """ Prepare Column names and column data types from schema file"""
        ipcolumns = []
        ipcolumn_types = {}
        
        with open(self.ipschemafile, 'r') as f:
            for line in f:
                if line == '':
                    break
                rec = line.strip().split()
                ipcolumns.append(rec[0])
                ipcolumn_types[rec[0]] = rec[1]
        # Raise exception if ipcolumns contain duplicates
        """ Create input file dataframe using dask"""
        ipdf = dd.read_csv(self.ipfile,
                           sep=self.sep,
                           skiprows=self.skiprows,
                           header=self.header,
                           names=ipcolumns,
                           compression=self.compression,
                           blocksize=self.blocksize,
                           dtype=ipcolumn_types)
        ipdf = ipdf.repartition(self.parallel)
        print("Time taken : {} for reading file '{}'".format(timeit.default_timer() - t1, self.ipfile))
        return ipdf
