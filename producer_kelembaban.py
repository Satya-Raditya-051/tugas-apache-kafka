from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

Id_gudang = ['G1', 'G2', 'G3']

while True:
    data = {"gudang_id": random.choice(Id_gudang), "kelembaban": random.randint(40, 80)}
    producer.send('sensor-kelembaban-gudang', value=data)
    print(f"Sent Kelembaban: {data}")
    time.sleep(1)
