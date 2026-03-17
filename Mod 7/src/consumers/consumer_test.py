from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'green-trips',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest'
)

for msg in consumer:
    print(msg.value)  # bytes
    print(msg.value.decode('utf-8'))
    break