import json
import time
import dataclasses
from pathlib import Path
import pandas as pd
from dataclasses import dataclass
from kafka import KafkaProducer

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


def green_ride_serializer(ride: GreenRide) -> bytes:
    return json.dumps(dataclasses.asdict(ride)).encode('utf-8')


def green_ride_from_row(row):
    return GreenRide(
        lpep_pickup_datetime=str(row['lpep_pickup_datetime']),
        lpep_dropoff_datetime=str(row['lpep_dropoff_datetime']),
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        passenger_count=int(row['passenger_count'] if type(row['passenger_count']) == int else 0),
        trip_distance=float(row['trip_distance']),
        tip_amount=float(row['tip_amount']),
        total_amount=float(row['total_amount']),
    )


def load_data():
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
    local_path = Path('data') / 'green_tripdata_2025-10.parquet'

    columns = [
        'lpep_pickup_datetime',
        'lpep_dropoff_datetime',
        'PULocationID',
        'DOLocationID',
        'passenger_count',
        'trip_distance',
        'tip_amount',
        'total_amount',
    ]

    if local_path.exists():
        df = pd.read_parquet(local_path, columns=columns)
    else:
        print("File not found locally. Downloading...")
        local_path.parent.mkdir(exist_ok=True)
        df = pd.read_parquet(url, columns=columns)
        df.to_parquet(local_path, index=False)

    return df


def create_producer():
    server = 'localhost:9092'
    return KafkaProducer(
        bootstrap_servers=[server],
        value_serializer=green_ride_serializer,
    )


def send_rides_fast(df, producer, topic_name='green-trips', delay=0.001):
    rides = [green_ride_from_row(row) for row in df.to_dict(orient='records')]
    count = 0

    t0 = time.time()
    for ride in rides:
        producer.send(topic_name, value=ride)
        count += 1
        if delay > 0:
            time.sleep(delay)
    producer.flush()
    t1 = time.time()

    print(f"Sent {count} rides in {(t1 - t0):.2f} seconds")


if __name__ == "__main__":
    df = load_data()
    producer = create_producer()
    print("Producer started. Sending data...")

    try:
        send_rides_fast(df, producer, delay=0.001)
        print("All data sent successfully!")
    except KeyboardInterrupt:
        producer.flush()
        print("\nProducer stopped by user!")