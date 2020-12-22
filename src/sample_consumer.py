from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json
import pandas as pd
from dask.distributed import Client

bootstrap_servers_list =  "localhost:9092"
topic = "sample_csv_file"
topic_group_id = "sample_read"

client = Client(processes=False)

class ConsumeEvents():

    def __init__(self, ip_topic_name, ip_topic_partition, ip_topic_group_id=None, 
                 op_topic_name=None, op_topic_group_id=None, producer_name=None, df_name=None, consumer_name=None):
        
        self.ip_topic_name = ip_topic_name
        self.ip_topic_partition = ip_topic_partition
        self.op_topic_name = op_topic_name
        self.ip_topic_group_id = ip_topic_group_id
        self.op_topic_group_id = op_topic_group_id
        self.producer_name = producer_name
        #Dynamic consumer and dataframe to be used for consuming messages
        self.consumer_name = "consumer_" + self.ip_topic_name + "_" + str(self.ip_topic_partition)
        self.df_name = self.consumer_name + "_" + self.ip_topic_name + "_df"
        


    def consume_events_and_publish(self):
        """ Initiate consumer for reading messages from input topic"""
        try:
            self.consumer_name = KafkaConsumer(
                bootstrap_servers=bootstrap_servers_list, 
                group_id=self.ip_topic_group_id,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        except:
            print("Error!!! Unable to initialize consumer")

        tp = TopicPartition(self.ip_topic_name,self.ip_topic_partition)
        """ assign partition to consumer"""
        self.consumer_name.assign([tp])
        """ obtain the last offset value"""
        self.consumer_name.seek_to_end(tp)
        lastOffset = self.consumer_name.position(tp)
        self.consumer_name.seek_to_beginning(tp)   

        if  lastOffset == 0 :
            self.consumer_name.close()
            print("No messages to consume from partition: ", self.ip_topic_partition)
        else:
            try:
                for message in self.consumer_name:
                    print("Offset:", message.offset)
                    print("Partition:", self.ip_topic_partition)
                    
                    """ Apply Transformation to the incoming messages and publish them to output topic"""
                    df = self.df_name
                    df = pd.read_json(json.dumps(message.value))
                    print(len(df.index))

                    """ Consumer reached end of reading producer topic messages"""
                    if message.offset == lastOffset - 1:
                        break
            except:
                self.consumer_name.close()

            """ Close the consumer as soon as its completed reading messages from input topic"""
            self.consumer_name.close()

for partition in range(4):
    ConsumeEvents(topic, partition, topic_group_id).consume_events_and_publish()



