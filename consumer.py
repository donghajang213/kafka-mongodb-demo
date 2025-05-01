from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# ✅ MongoDB 연결 설정
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['kafka_demo']                # DB 이름
collection = db['messages']                   # Collection 이름

# ✅ Kafka Consumer 설정
consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='python-consumer'
)

print("👀 Kafka consumer 시작... 메시지를 MongoDB에 저장 중...")

for msg in consumer:
    decoded = msg.value.decode('utf-8')
    print(f"📥 Received: {decoded}")
    
    # ✅ MongoDB에 저장
    collection.insert_one({'message': decoded})
