from kafka import KafkaConsumer, KafkaProducer
import json
import readtextfile

bootstrap_servers_list =  "localhost:9092"
topic_name = 'sample_csv_file'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers_list, 
    value_serializer=lambda x : json.dumps(x).encode('utf-8'))

df = readtextfile.ReadTextFile(ipfile='/Users/sriyan/Downloads/1500000_Sales_Records.csv',
                                   ipschemafile='schema/sample_csv_file.txt',
                                   delimiter=',', skiprows=1, parallel=4).read_using_dask()

for i in range(df.npartitions):
    producer.send(topic_name, df.get_partition(i).compute().to_dict(orient="records"))

producer.close()
print("All messages published to input topic")
