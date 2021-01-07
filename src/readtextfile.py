# Copyright 2020 The Nadi Data Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import os
import io
import timeit
import dask.dataframe as dd
import pandas as pd


class ReadTextFile():

    def __init__(self, ipfile, ipschemafile, delimiter, engine='c', skiprows=0, nrows=0, header=None,
                 compression=None, index_col=None, blocksize=None, skipfooter=0, parallel=4,
                 parse_dates=None):
        self.ipfile = ipfile
        self.ipschemafile = ipschemafile
        self.sep = delimiter
        self.engine = engine
        self.skiprows = skiprows
        self.nrows = nrows
        self.header = header
        self.compression = compression
        self.index_col = index_col
        self.blocksize = blocksize
        self.skipfooter = skipfooter
        self.parallel = parallel
        self.parse_dates = parse_dates

    def read_using_dask(self):
        t1 = timeit.default_timer()
        """ Prepare Column names and column data types from schema file"""
        ipcolumns = []
        ipdatetimecolumns = []
        ipcolumnwidths = []
        ipcolumn_types = {}
        # all empty lines should be removed from ipschemafile
        with open(self.ipschemafile, 'r') as f:
            for line in f:
                if line == '':
                    break
                rec = line.strip().split()
                ipcolumns.append(rec[0])
                if self.sep == 'fixed width':
                    ipcolumnwidths.append(int(rec[1]))
                    if rec[2] == 'date':
                        ipdatetimecolumns.append(rec[0])
                        ipcolumn_types[rec[0]] = 'string' #Taking more time when parsing dates
                    else:
                        ipcolumn_types[rec[0]] = rec[2]
                else:
                    if rec[1] == 'date':
                        ipdatetimecolumns.append(rec[0])
                        ipcolumn_types[rec[0]] = 'string' #Taking more time when parsing dates
                    else:
                        ipcolumn_types[rec[0]] = rec[1]

        # Raise exception if ipcolumns contain duplicates

        if self.sep == 'fixed width':
            """ Create input file dataframe using dask read fwf"""
            ipdf = dd.read_fwf(self.ipfile,
                               header=self.header,
                               names=ipcolumns,
                               dtype=ipcolumn_types,
                               widths=ipcolumnwidths)
            ipdf = ipdf.repartition(self.parallel)
        else:
            """ Create input file dataframe using dask read csv"""
            ipdf = dd.read_csv(self.ipfile,
                               sep=self.sep,
                               skiprows=self.skiprows,
                               header=self.header,
                               names=ipcolumns,
                               compression=self.compression,
                               blocksize=self.blocksize,
                               #parse_dates=ipdatetimecolumns,
                               dtype=ipcolumn_types)
            ipdf = ipdf.repartition(self.parallel)
        print("Time taken : {} seconds for reading file '{}'".format(
            timeit.default_timer() - t1, self.ipfile))
        return ipdf

    def read_using_dask_header(self):
        t1 = timeit.default_timer()
        """ Prepare Column names and column data types from schema file"""
        ipcolumns = []
        ipcolumn_types = {}
        # all empty lines should be removed from ipschemafile
        with open(self.ipschemafile, 'r') as f:
            for line in f:
                if line == '':
                    break
                rec = line.strip().split()
                ipcolumns.append(rec[0])
                ipcolumn_types[rec[0]] = rec[1]
        # Raise exception if ipcolumns contain duplicates

        """ Read Header"""
        with open(self.ipfile, 'r') as f:
            first_line = f.readline()

        """ Create input file dataframe using dask"""
        ipdf_header = pd.read_csv(io.StringIO(first_line),
                                  sep=self.sep,
                                  names=ipcolumns,
                                  compression=self.compression,
                                  dtype=ipcolumn_types)

        print("Time taken : {} seconds for reading header '{}'".format(
            timeit.default_timer() - t1, self.ipfile))
        return ipdf_header

    def read_using_dask_trailer(self):
        t1 = timeit.default_timer()
        """ Prepare Column names and column data types from schema file"""
        ipcolumns = []
        ipcolumn_types = {}
        # all empty lines should be removed from ipschemafile
        with open(self.ipschemafile, 'r') as f:
            for line in f:
                if line == '':
                    break
                rec = line.strip().split()
                ipcolumns.append(rec[0])
                ipcolumn_types[rec[0]] = rec[1]
        # Raise exception if ipcolumns contain duplicates

        """ Read Trailer"""
        with open(self.ipfile, 'rb') as f:
            f.seek(-2, os.SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR)
            last_line = f.readline().decode()

        """ Create input file dataframe using dask"""
        ipdf_trailer = pd.read_csv(io.StringIO(last_line),
                                   sep=self.sep,
                                   names=ipcolumns,
                                   compression=self.compression,
                                   dtype=ipcolumn_types)

        print("Time taken : {} seconds for reading trailer '{}'".format(
            timeit.default_timer() - t1, self.ipfile))
        return ipdf_trailer

    def read_using_dask_detail(self):
        t1 = timeit.default_timer()
        """ Prepare Column names and column data types from schema file"""
        ipcolumns = []
        ipcolumn_types = {}
        # all empty lines should be removed from ipschemafile
        with open(self.ipschemafile, 'r') as f:
            for line in f:
                if line == '':
                    break
                rec = line.strip().split()
                ipcolumns.append(rec[0])
                ipcolumn_types[rec[0]] = rec[1]
        # Raise exception if ipcolumns contain duplicates
        """ Create input file dataframe using dask"""
        ipdf_detail = dd.read_csv(self.ipfile,
                                  sep=self.sep,
                                  skiprows=self.skiprows,
                                  header=self.header,
                                  names=ipcolumns,
                                  compression=self.compression,
                                  blocksize=self.blocksize,
                                  skipfooter=self.skipfooter,
                                  engine=self.engine,
                                  dtype=ipcolumn_types)
        ipdf_detail = ipdf_detail.repartition(self.parallel)
        print("Time taken : {} seconds for reading detail records in a file '{}'".format(
            timeit.default_timer() - t1, self.ipfile))
        return ipdf_detail
