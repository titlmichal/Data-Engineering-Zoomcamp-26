import json
import dataclasses
from dataclasses import dataclass

@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    trip_distance: float
    total_amount: float
    tpep_pickup_datetime: int # epoch in ms

def row_to_ride(row):
    return Ride(
        PULocationID=int(row.PULocationID),
        DOLocationID=int(row.DOLocationID),
        trip_distance=float(row.trip_distance),
        total_amount=float(row.total_amount),
        tpep_pickup_datetime=int(row.tpep_pickup_datetime.timestamp() * 1000)
    )

def ride_serializer(ride):
    return json.dumps(dataclasses.asdict(ride)).encode("utf-8")

def ride_deserializer(ride_bytes):
    return Ride(**json.loads(ride_bytes.decode("utf-8")))