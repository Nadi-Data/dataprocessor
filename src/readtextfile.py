# Copyright 2020 The Nadi Data Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import dask.dataframe as dd

class ReadTextFile():

    def __init__(self, ipfile, ipschemafile, delimiter, skiprows=0, header=None, index_col=False):
        self.ipfile = ipfile
        self.ipschemafile = ipschemafile
        self.sep = delimiter
        self.skiprows = skiprows
        self.header = header

    def read_using_dask(self):
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
                           dtype=ipcolumn_types)
        return ipdf
