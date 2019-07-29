import json
from kafka import KafkaConsumer
from kafka import KafkaProducer
import time
import sys

if len(sys.argv) != 2:
    print("invalid params: python kafka.writer.py alert_info")
    exit(-1)

alert_info_file = sys.argv[1]
print 'alert_info_file', alert_info_file

alerts_dict = {
    "message":[
        {
            "name":"model-offlinefeature",
            "tags":{
                "metric":"error"
            },
            "time":"2019-07-27T17:29:40+08:00",
            "values":{
                "c":1,
                "info":""
            }
        }
    ],
    "topic":"test"
}

#producer = KafkaProducer(bootstrap_servers=['172.25.20.106:9092'])
producer = KafkaProducer(bootstrap_servers=['bigdatacpc.qtt.prd.1sapp.com:9092'])
time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
date = time_str.split(" ")[0]
time = time_str.split(" ")[1]
time_str_latest = date + "T" + time + "+08:00"
alerts_dict["message"][0]["time"] = time_str_latest

with open(alert_info_file, "r") as fr:
    for line in fr:
        alerts_dict["message"][0]["values"]["info"] = line.strip()
        alerts_json = json.dumps(alerts_dict)
        print(alerts_json)
        #future = producer.send('test', value= b'' + alerts_json)
        future = producer.send('topic:alarm_error', value= b'' + alerts_json)
        result = future.get(timeout=5)
        print(result)


#alerts_json = json.dumps(alerts_dict)
#print(alerts_json)
#future = producer.send('test', value= b'' + alerts_json)
#result = future.get(timeout=10)
#print(result)

#for alert in alerts:
#    alerts_dict["message"][0]["values"]["info"] = alert
#    alerts_json = json.dumps(alerts_dict)
#    print(alerts_json)
#    future = producer.send('test', value= b'' + alerts_json)
#    result = future.get(timeout=20)
#    print(result)



#for alert in alerts:
#    future = producer.send('test', value= b'' + alert)
#    result = future.get(timeout=20)
#    print(result)

#consumer = KafkaConsumer('test', bootstrap_servers= ['172.25.20.106:9092'])
#for msg in consumer:
#    print(msg)