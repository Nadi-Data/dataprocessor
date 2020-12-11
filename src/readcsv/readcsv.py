# Copyright 2020 The Nadi Data Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

class Readcsv():
    
    def __init__(self, file, read_mode=None, chunk_size=None, row_sep=None, delim=None):
        self.file = file
        self.read_mode = read_mode
        self.chunk_size = chunk_size
        self.row_sep = row_sep
        self.delim = delim

    def read_in_chunks(self, f):
        """Read file in chunks for processing"""
        curr_rec = ''
        while True:
            chunk = f.read(self.chunk_size)
            if chunk == '': # End of the file
                yield curr_rec
                break
            while True:
                i = chunk.find(self.row_sep)
                if i == -1:
                    break
                yield curr_rec + chunk[:i]
                curr_rec = ''
                chunk = chunk[i+1:]
            curr_rec += chunk


    def process_chunks_in_parallel(self):
        with open(self.file, self.read_mode) as f:
            row_count = 0
            for line in self.read_in_chunks(f):
                if line == '':
                    break
                record = line.strip().split(self.delim) # Convert the record into array using delimiter
                """ Convert Header into columns"""
                if row_count == 0:
                    header = record
                    row_count += 1
                    continue
                else:
                    """ Prepare column , value pairs in the form of dictionary"""
                    record = dict(zip(header,record))
                    yield record
                    row_count += 1