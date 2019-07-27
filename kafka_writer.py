import json
from kafka import KafkaConsumer
from kafka import KafkaProducer
import time


alerts  = [
"[Invalid Variety]value count(6831) of feature dtu_id exceeds 120% of average count(6630) of past 15 days",
"[Invalid Variety]value count(4073) of feature unitid exceeds 120% of average count(3573) of past 15 days",
"[Invalid Variety]value count(1795) of feature userid exceeds 120% of average count(1639) of past 15 days",
"[Invalid Variety]value count(1490) of feature site_id exceeds 120% of average count(1348) of past 15 days",
"[Invalid Variety]value count(3832) of feature channel exceeds 120% of average count(3486) of past 15 days",
"[Invalid Variety]value count(13469) of feature ideaid less than 80% of average count(25710) of past 15 days",
"[Invalid Variety]value count(3588) of feature planid exceeds 120% of average count(3105) of past 15 days",
"[Invalid Variety]value count(79) of feature adclass exceeds 120% of average count(72) of past 15 days"
]

alerts_json = {
    "message":[
        {
            "name":"model-offlinefeature",
            "tags":{
                "metric":"error"
            },
            "time":"2019-07-27T17:29:40+08:00",
            "values":{
                "c":1,
                "info":alerts[0]
            }
        }
    ],
    "topic":"test"
}


json = json.dumps(alerts_json)
print(json)
localtime = time.localtime(time.time())
print localtime

time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
date = time_str.split(" ")
time = time_str.split(" ")
print date
print time


producer = KafkaProducer(bootstrap_servers=['172.25.20.106:9092'])
future = producer.send('test', value= b'' + json)
result = future.get(timeout=20)
print(result)

#for alert in alerts:
#    future = producer.send('test', value= b'' + alert)
#    result = future.get(timeout=20)
#    print(result)

#consumer = KafkaConsumer('test', bootstrap_servers= ['172.25.20.106:9092'])
#for msg in consumer:
#    print(msg)