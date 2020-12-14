# Copyright 2020 The Nadi Data Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import timeit
import dask.dataframe as dd


class WriteTextFile():

    def __init__(self, ipdf, filename, single_file=True, sep=',', header=False, encoding='utf-8', mode='wt', compression=None, index=False,compute=True):
        self.ipdf = ipdf
        self.filename = filename
        self.single_file = single_file
        self.sep=sep
        self.header = header
        self.encoding = encoding
        self.mode = mode
        self.compression = compression
        self.index = index
        self.compute = compute

    def write_using_dask(self):
        t1 = timeit.default_timer()
        """ Write output file using dask"""
        self.ipdf.to_csv(filename=self.filename,
                         single_file=self.single_file,
                         sep=self.sep,
                         header=self.header,
                         encoding=self.encoding,
                         mode=self.mode,
                         compute=self.compute,
                         index=self.index,
                         compression=self.compression)
        print("Time taken : {} for writing file '{}'".format(timeit.default_timer() - t1, self.filename))
