# Copyright 2020 The Nadi Data Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

from kafka import KafkaProducer
import json
import os
import readtextfile
import readcsv


class ReadandDistribute():

    def __init__(self, bootstrap_servers_list, ipfile, ipschemafile, delimiter, skiprows=0, parallel=4, compression=None,
                 ipdf_name=None, ip_producer_name=None, ip_topic_name=None):
        self.ipfile = ipfile
        self.ipschemafile = ipschemafile
        self.delimiter = delimiter
        self.skiprows = skiprows
        self.compression = compression
        self.parallel = parallel
        self.bootstrap_servers_list = bootstrap_servers_list
        self.ip_topic_name = ip_topic_name
        self.ipdf_name = str(os.path.basename(ipfile).split(".")[0]) + "_df"
        self.ip_producer_name = "producer_" + \
            str(os.path.basename(ipfile).split(".")[0])

    def read_text_using_dask(self):

        self.ip_producer_name = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers_list,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'))

        self.ipdf_name = readtextfile.ReadTextFile(self.ipfile, self.ipschemafile, self.delimiter,
                                                   self.skiprows, self.parallel, self.compression).read_using_dask()
        
        for i in range(self.ipdf_name.npartitions):
            self.ip_producer_name.send(self.ip_topic_name, self.ipdf_name.get_partition(i).compute().to_json(orient="records"))

        self.ip_producer_name.close()

    def read_text_using_yield(self):

        readcsv.ReadTextYield(self.ipfile,'r' ,5000 ,'\n' ,self.delimiter).process_chunks_in_parallel()


