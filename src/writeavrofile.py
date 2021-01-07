# Copyright 2020 The Nadi Data Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import timeit
import dask.dataframe as dd
from fastavro import writer
#import pandavro

class WriteAvroFile():

    def __init__(self, ipdf, client, opfile=None, compression=None, append=False, overwrite=False, compute=True):
        
        self.ipdf = ipdf
        self.opfile = opfile
        self.compression = compression
        self.append = append
        self.overwrite = overwrite
        self.compute = compute
        self.client = client
        self.schema = ''

        with open('/Users/sripri/Documents/dataprocessor/schema/sample_csv_file.schema', 'r') as f:
            schema_fields = []
            for line in f.readlines():
                l = line.strip().split()
                if l[1] != 'date':
                    schema_fields.append({'name': l[0], 'type':['null', {'type': l[1]}]})
                else:
                    schema_fields.append({'name': l[0], 'type':['null', {'type': 'int' , 'logicalType': l[1]}]})
        self.schema = {'name': 'avro schema', 'doc': 'generated automatically using dataprocessor',
                        'type': 'record',
                        'fields': schema_fields}

    def write_using_fastavro(self):
        t1 = timeit.default_timer()
        file_nm = '/Users/sripri/Downloads/sample_file.' + str(i) + '.avro'
        with open(file_nm, 'wb') as f:
            writer(f, schema=self.schema, records=self.ipdf.get_partition(i).compute().to_dict("records"))
        #pandavro.to_avro(file_nm, self.ipdf.get_partition(i).compute(), schema=schema, append=self.append)
        print("Time taken : {} seconds ".format(timeit.default_timer() - t1))
        return 1
