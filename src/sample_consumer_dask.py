from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json
import pandas as pd
from dask.distributed import Client

bootstrap_servers_list = "localhost:9092"
topic = "sample_csv_file"
topic_group_id = "sample_read"
op_topic_name = 'sample_txt_file'

client = Client(processes=False)


def consume_events_and_publish(ip_topic_name, ip_topic_partition, ip_topic_group_id=None, bootstrap_servers_list=None,
                            op_topic_name=None, op_topic_group_id=None, producer_name=None, df_name=None,
                            consumer_name=None):

    # Dynamic consumer and dataframe to be used for consuming messages
    consumer_name = "consumer_" + ip_topic_name + \
        "_" + str(ip_topic_partition)
    df_name = consumer_name + "_" + ip_topic_name + "_df"

    """ Initiate consumer for reading messages from input topic"""
    try:
        consumer_name = KafkaConsumer(
            bootstrap_servers=bootstrap_servers_list,
            group_id=ip_topic_group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    except:
        print("Error!!! Unable to initialize consumer")

    tp = TopicPartition(ip_topic_name, ip_topic_partition)
    """ assign partition to consumer"""
    consumer_name.assign([tp])
    """ obtain the last offset value"""
    consumer_name.seek_to_end(tp)
    lastOffset = consumer_name.position(tp)
    consumer_name.seek_to_beginning(tp)

    if lastOffset == 0:
        consumer_name.close()
        print("No messages to consume from partition: ", ip_topic_partition)
    else:
        try:
            for message in consumer_name:
                print("Offset:", message.offset)
                print("Partition:", ip_topic_partition)

                """ Apply Transformation to the incoming messages and publish them to output topic"""
                df_name = pd.read_json(json.dumps(message.value))
                df_name = df_name[df_name["Region"] == "Europe"]
                producer_name.send(op_topic_name, df_name.to_dict(orient="records"))

                """ Consumer reached end of reading producer topic messages"""
                if message.offset == lastOffset - 1:
                    break
        except:
            consumer_name.close()

    """ Close the consumer as soon as its completed reading messages from input topic"""
    consumer_name.close()

p0 = client.submit(consume_events_and_publish, topic, 0, topic_group_id, bootstrap_servers_list)

p1 = client.submit(consume_events_and_publish, topic, 1, topic_group_id, bootstrap_servers_list)

p2 = client.submit(consume_events_and_publish, topic, 2, topic_group_id, bootstrap_servers_list)

p3 = client.submit(consume_events_and_publish, topic, 3, topic_group_id, bootstrap_servers_list)