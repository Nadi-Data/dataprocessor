from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer, KafkaProducer
import json

bootstrap_servers_list =  "localhost:9092"
topic_list = ['sample_csv_file', 'sample_text_file']

admin_client = KafkaAdminClient(
    bootstrap_servers=bootstrap_servers_list, 
    client_id='test'
)
admin_consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers_list)

#admin_client.delete_topics(topic_list)

topic_list_final = set(topic_list) - set(admin_consumer.topics())
topic_list_create=[]
for topic_name in topic_list_final:
    topic_list_create.append(NewTopic(name=topic_name, num_partitions=4, replication_factor=1))
admin_client.create_topics(new_topics=topic_list_create, validate_only=False)

admin_client.close()
admin_consumer.close()
