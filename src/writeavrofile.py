# Copyright 2020 The Nadi Data Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import timeit
import dask.dataframe as dd
from fastavro import writer
#import pandavro

class WriteAvroFile():

    def __init__(self, ipdf, partitionnumber, opfile=None, compression=None, append=False, overwrite=False, compute=True):
        
        self.ipdf = ipdf
        self.partitionnumber = partitionnumber
        self.opfile = opfile + '.' + str(self.partitionnumber) + '.avro'
        self.compression = compression
        self.append = append
        self.overwrite = overwrite
        self.compute = compute
        self.schema = ''

        self.ipdf.dtypes.apply(lambda x: x.name).to_dict()
        schema_fields = []
        for key, value in self.ipdf.dtypes.apply(lambda x: x.name).to_dict().items():
            if value in ['int64', 'int32']:
                value = 'int'
            elif value in ['float64', 'float32']:
                value = 'float'
            if value not in ['datetime64[ns]']:
                schema_fields.append({'name': key, 'type':['null', {'type': value}]})
            else:
                schema_fields.append({'name': key, 'type':['null', {'type': 'int' , 'logicalType': 'date'}]})
        self.schema = {'name': 'avro schema', 'doc': 'generated automatically using dataprocessor',
                        'type': 'record',
                        'fields': schema_fields}

    def write_using_fastavro(self):
        t1 = timeit.default_timer()
        with open(self.opfile, 'wb') as f:
            writer(f, schema=self.schema, records=self.ipdf.to_dict("records"))
        #pandavro.to_avro(self.opfile, self.ipdf, schema=self.schema, append=self.append)
        print("Time taken : {} seconds ".format(timeit.default_timer() - t1))
        return 1
