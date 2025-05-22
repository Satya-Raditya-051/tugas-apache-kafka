from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

id_gudang = ['G1', 'G2', 'G3']

while True:
    data = {"id_gudang": random.choice(id_gudang), "suhu": random.randint(20, 90)}
    producer.send('sensor-suhu-gudang', value=data)
    print(f"Sent Suhu: {data}")
    time.sleep(1)
