
from kafka import KafkaConsumer
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['172.25.20.106:9092'])
future = producer.send('test', value= b'my_value')
result = future.get(timeout=20)
print(result)

consumer = KafkaConsumer('test', bootstrap_servers= ['172.25.20.106:9092'])
for msg in consumer:
    print(msg)