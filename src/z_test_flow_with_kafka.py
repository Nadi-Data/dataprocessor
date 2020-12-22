from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json
import pandas as pd
from dask.distributed import Client
import readtextfile
import writetextfile
import readanddistribute

bootstrap_servers_list = "localhost:9092"
topic = "sample_csv_file"
topic_group_id = "sample_read"
op_topic_name = 'sample_text_file'

client = Client(processes=False)

""" Read file and publish to topic"""
readanddistribute.ReadandDistribute(ipfile='/Users/sriyan/Downloads/1500000_Sales_Records.csv',
                                    ipschemafile='schema/sample_csv_file.txt',
                                    delimiter=',', skiprows=1, parallel=4,
                                    ip_topic_name=topic,
                                    bootstrap_servers_list=bootstrap_servers_list).read_text()


def consume_events_and_publish(ip_topic_name, ip_topic_partition, ip_topic_group_id, bootstrap_servers_list,
                               op_topic_name=None, op_topic_group_id=None, producer_name=None, df_name=None,
                               consumer_name=None):

    # Dynamic consumer and dataframe to be used for consuming messages
    consumer_name = "consumer_" + str(ip_topic_name) + \
        "_" + str(ip_topic_partition)
    producer_name = "producer_" + str(op_topic_name)
    df_name = consumer_name + "_" + ip_topic_name + "_df"

    """ Initiate a consumer for reading messages from input topic"""
    try:
        consumer_name = KafkaConsumer(bootstrap_servers=bootstrap_servers_list,
                                      group_id=ip_topic_group_id,
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
    consumer_name.seek_to_end(tp)
    lastOffset = consumer_name.position(tp)
    consumer_name.seek_to_beginning(tp)

    print(lastOffset)

    if lastOffset == 0:
        consumer_name.close()
        #print("No messages to consume from partition: ", ip_topic_partition)
    else:
        try:
            for message in consumer_name:
                print("Offset:", message.offset)
                print("Partition:", ip_topic_partition)
                # print(message.value)
                """ Apply Transformation to the incoming messages and publish them to output topic"""
                df = pd.read_json(message.value)
                # print(df.index)
                df = df[df['Region'] == 'Europe']
                # print(df.to_json(orient="records"))
                producer_name.send(op_topic_name, df.to_json(orient="records"))

                """ Consumer reached end of reading producer topic messages"""
                if message.offset == lastOffset - 1:
                    break
        except:
            consumer_name.close()

    """ Close the consumer as soon as its completed reading messages from input topic"""
    consumer_name.close()


def write_text_file_from_topic(bootstrap_servers_list, ip_topic_name):

    # Dynamic consumer and dataframe to be used for consuming messages
    consumer_name = "consumer_" + ip_topic_name
    op_file_name = "/Users/sriyan/Downloads/" + "op_file_" + ip_topic_name + ".txt"

    """ Initiate consumer for reading messages from input topic"""
    try:
        consumer_name = KafkaConsumer(ip_topic_name,
                                      bootstrap_servers=bootstrap_servers_list,
                                      consumer_timeout_ms=10000,
                                      # auto_offset_reset='earliest',
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
                # print(message.value)
                df1 = pd.read_json(message.value)
                print(df1.head())
                df1.to_csv(op_file_name, sep='|', mode='a',
                           index=False, encoding='utf-8', header=False)
        except:
            consumer_name.close()

    """ Close the consumer as soon as its completed reading messages from input topic"""
    consumer_name.close()


p0 = client.submit(consume_events_and_publish, topic, 0, topic_group_id, bootstrap_servers_list,op_topic_name)
p1 = client.submit(consume_events_and_publish, topic, 1, topic_group_id, bootstrap_servers_list,op_topic_name)
p2 = client.submit(consume_events_and_publish, topic, 2, topic_group_id, bootstrap_servers_list,op_topic_name)
p3 = client.submit(consume_events_and_publish, topic, 3, topic_group_id, bootstrap_servers_list,op_topic_name)
p0.result()
p1.result()
p2.result()
p3.result()

print("done")

p = client.submit(write_text_file_from_topic, bootstrap_servers_list, op_topic_name)

p.result()

print("done")

#consume_events_and_publish(topic, 0, topic_group_id,bootstrap_servers_list, op_topic_name)
#write_text_file_from_topic(bootstrap_servers_list, op_topic_name)
