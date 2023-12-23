import json
from kafka import KafkaConsumer
import influxdb_client
import os
import time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


KAFKA_TRANSACT_PRODUCER_TOPIC = "orders_confirmed"
BUCKET_NAME = "Orders"
consumer = KafkaConsumer(KAFKA_TRANSACT_PRODUCER_TOPIC,
                         bootstrap_servers="3.110.81.255:9092")

totalOrdersCount = 0
totalRevenue = 0

print("Listening to Order Status for updating the revenue analytis....")

token = os.environ.get("INFLUXDB_TOKEN")
token = "7Y20YLFYB_BNaX4FXfsMA7CSauB-GeMGQmHrOpngPfnHOc7RR2Q1AJln3jXcv5lSy3s1fgZMtUzaoR7kPjSBhQ=="
org = "Clowns"
url = "http://3.110.81.255:8086"

write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
write_api = write_client.write_api(write_options=SYNCHRONOUS)
while True:
    for message in consumer:
        print("Updating Analytics...")
        consumedMessage = json.loads(message.value.decode())
        totalRevenue += consumedMessage["TotalCost"]
        totalOrdersCount += 1
        print(f"Orders so far today {totalOrdersCount}")
        print(f"Total Revenue Generated today {totalRevenue}")
        print("Sending Analytics data as measurements to InfluxDB.....")

        point = (
            Point("revenue-metrics")
            .tag("customerid", consumedMessage["CustomerID"])
            .tag("customername", consumedMessage["CustomerName"])
            .field("totalOrdersCount", totalOrdersCount)
            .field("totalrevenue", totalRevenue)
        )

        write_api.write(bucket=BUCKET_NAME, org=org, record=point)

        time.sleep(2)
