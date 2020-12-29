from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json
import timeit
import pandas as pd
from dask.distributed import Client
import readtextfile
import writetextfile
import readcsv
import readanddistribute
import ray

ray.init()

t1 = timeit.default_timer()

ipfile = '/Users/sriyan/Downloads/sales_50mb_1500000.csv'
bootstrap_servers_list = "localhost:9092"
topic = "sample_csv_file"
topic_group_id = "sample_read"
op_topic_name = 'sample_text_file'

#client = Client(processes=False)

""" Read file and publish to topic"""
@ray.remote
def readanddistributemessages(ipfile):
    readcsv.ReadTextYield(ipfile,'r' ,50000 ,'\n' ,',').process_chunks_in_parallel()
    return 'read done'

@ray.remote
def consume_events_and_publish(ip_topic_name, ip_topic_partition, ip_topic_group_id, bootstrap_servers_list,
                               op_topic_name, producer_name=None, df_name=None,
                               consumer_name=None):

    # Dynamic consumer and dataframe to be used for consuming messages
    consumer_name = "consumer_" + str(ip_topic_name) + \
        "_" + str(ip_topic_partition)
    producer_name = "producer_" + str(op_topic_name)
    df_name = consumer_name + "_" + ip_topic_name + "_df"

    #print(consumer_name)
    #print(producer_name)
    #print(df_name)

    """ Initiate a consumer for reading messages from input topic"""
    try:
        consumer_name = KafkaConsumer(bootstrap_servers=bootstrap_servers_list,
                                      #consumer_timeout_ms=20000,
                                      #auto_offset_reset='earliest',
                                      #group_id=topic_group_id,
                                      value_deserializer=lambda x: json.loads(x.decode('utf-8')))                                      
    except:
        print("Error!!! Unable to initialize consumer")

    """ Initiate a producer for sending transformed messages to output topic"""
    try:
        producer_name = KafkaProducer(bootstrap_servers=bootstrap_servers_list,
                                      value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    except:
        print("Error!!! Unable to initiate producer")

    tp = TopicPartition(ip_topic_name, ip_topic_partition)
    """ assign partition to consumer"""
    consumer_name.assign([tp])
    """ obtain the last offset value"""
    #consumer_name.seek_to_end(tp)
    #lastOffset = consumer_name.position(tp)
    #consumer_name.seek_to_beginning(tp)
    x = 1
    if x == 0:
        consumer_name.close()
        #print("No messages to consume from partition: ", ip_topic_partition)
    else:
        try:
            for message in consumer_name:
                #print("Offset:", message.offset)
                print("Partition:", ip_topic_partition)
                # print(message.value)
                """ Apply Transformation to the incoming messages and publish them to output topic"""
                df = pd.read_json(message.value)
                # print(df.index)
                df = df[df['Region'] == 'Europe']
                # print(df.to_json(orient="records"))
                producer_name.send(op_topic_name, df.to_json(orient="records"))
        except:
            consumer_name.close()

    """ Close the consumer as soon as its completed reading messages from input topic"""
    consumer_name.close()
    producer_name.close()
    return 'filter done'

@ray.remote
def write_text_file_from_topic(bootstrap_servers_list, ip_topic_name):

    # Dynamic consumer and dataframe to be used for consuming messages
    consumer_name = "consumer_" + ip_topic_name
    op_file_name = "/Users/sriyan/Downloads/" + "op_file_" + ip_topic_name + ".txt"

    """ Initiate consumer for reading messages from input topic"""
    try:
        consumer_name = KafkaConsumer(ip_topic_name,
                                      bootstrap_servers=bootstrap_servers_list,
                                      #consumer_timeout_ms=10000,
                                      #auto_offset_reset='earliest',
                                      group_id='sample-write',
                                      value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    except:
        print("Error!!! Unable to initialize consumer")

    consumer_name.poll()

    x = 1

    if x == 0:
        consumer_name.close()
        #print("No messages to consume from partition: ", ip_topic_partition)
    else:
        try:
            """ Write data to a file from input topic"""
            #with open(op_file_name, 'w'):
            for message in consumer_name:
                #print("Offset:", message.offset)
                print(message.value)
                df1 = pd.read_json(message.value)
                #print(df1.head())
                df1.to_csv(op_file_name, sep='|', mode='a',
                           index=False, encoding='utf-8', header=False)
        except:
            consumer_name.close()

    """ Close the consumer as soon as its completed reading messages from input topic"""
    consumer_name.close()
    return 'write done'

result_ids=[]

result_ids.append(readanddistributemessages.remote(ipfile))

for i in range(4):
    result_ids.append(consume_events_and_publish.remote(topic, i, topic_group_id, bootstrap_servers_list,op_topic_name))

result_ids.append(write_text_file_from_topic.remote(bootstrap_servers_list, op_topic_name))

results = ray.get(result_ids)

print(results)

"""
p00 = client.submit(readanddistributemessages, ipfile, priority=1)

p0 = client.submit(consume_events_and_publish, topic, 0, topic_group_id, bootstrap_servers_list,op_topic_name, priority=2)
p1 = client.submit(consume_events_and_publish, topic, 1, topic_group_id, bootstrap_servers_list,op_topic_name, priority=2)
p2 = client.submit(consume_events_and_publish, topic, 2, topic_group_id, bootstrap_servers_list,op_topic_name, priority=2)
p3 = client.submit(consume_events_and_publish, topic, 3, topic_group_id, bootstrap_servers_list,op_topic_name, priority=2)



p = client.submit(write_text_file_from_topic, bootstrap_servers_list, op_topic_name, priority=2)

p00.result()
p0.result()
p1.result()
p2.result()
p3.result()

print("done")
p.result()
"""

print("Time taken : {} seconds for z_test_flow_with_kafka".format(timeit.default_timer() - t1))
