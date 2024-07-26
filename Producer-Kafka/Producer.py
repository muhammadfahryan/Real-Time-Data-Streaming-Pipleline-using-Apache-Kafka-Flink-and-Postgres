from confluent_kafka import Producer
from faker import Faker
import random
import time
import schedule
import json

kafka_broker = "localhost:9092"
Topic = "weathers"

def gen_data():
    faker = Faker()
    
    # Perbaiki parameter konfigurasi
    prod = Producer({'bootstrap.servers': kafka_broker})
    my_data = {'city': faker.city(), 'temperature': random.uniform(10.0, 110.0)}
    print(my_data)
    
    json_data = json.dumps(my_data)
    # Mengubah dict menjadi string sebelum mengirim
    prod.produce(Topic, value=json_data)
    prod.flush()
    
if __name__ == "__main__":
    gen_data()
    schedule.every(10).seconds.do(gen_data)
    
    while True:
        schedule.run_pending()
        time.sleep(0.5)
