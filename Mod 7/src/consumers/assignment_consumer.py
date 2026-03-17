import sys
from pathlib import Path
from kafka import KafkaConsumer
from dataclasses import dataclass
import json

@dataclass
class GreenRide:
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str
    PULocationID: int
    DOLocationID: int
    passenger_count: int
    trip_distance: float
    tip_amount: float
    total_amount: float


def green_ride_deserializer(data: bytes) -> GreenRide:
    return GreenRide(**json.loads(data.decode('utf-8')))

count = 0
total = 0

def main():
    server = 'localhost:9092'
    consumer = KafkaConsumer(
        'green-trips',
        bootstrap_servers=[server],
        auto_offset_reset='earliest',
        group_id='green-rides-counter',
        value_deserializer=green_ride_deserializer
    )

    global count
    print("Consumer started. Listening for messages... (Ctrl+C to stop)\n")

    for message in consumer:
        ride = message.value
        if ride.trip_distance > 5:
            count += 1

    # consumer.close()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\nConsumer stopped by user!")

    print(f"\nTotal green taxi rides over 5 km: {count}")