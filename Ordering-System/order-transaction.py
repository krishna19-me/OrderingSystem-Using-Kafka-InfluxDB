from faker import Faker
import json
import datetime
from time import sleep
import random
from kafka import KafkaConsumer, KafkaProducer

KAFKA_CONSUMER_TOPIC = "orders"
KAFKA_TRANSACT_PRODUCER_TOPIC = "orders_confirmed"

ordersConsumer = KafkaConsumer(
    KAFKA_CONSUMER_TOPIC, bootstrap_servers="3.110.81.255:9092")

ordersConfirmedProducer = KafkaProducer(bootstrap_servers="3.110.81.255:9092",
                                        value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'))


print("Listening to Produced Orders.....")

transactionMessages = ["Confirmed", "Cancelled", "Processed"]


while True:
    for message in ordersConsumer:
        transactMessage = random.choice(transactionMessages)
        if transactMessage == "Confirmed":
            orderStatus = "Your Order has been Confirmed! Thanks ❤️"
        elif transactMessage == "Cancelled":
            orderStatus = "You Order has been Cancelled Successfully. Please leave a Feedback 😊"
        else:
            orderStatus = "You Order is in Processing Stage. Thanks for the Patience 🙇‍♂️"
        print("Ongoing Transaction...")
        consumedMessage = json.loads(message.value.decode())
        # print(consumedMessage)

        transactOrdersMessage = {
            "CustomerID": consumedMessage["OrderID"],
            "CustomerName": consumedMessage["Name"],
            "CustomerEmail": consumedMessage["Email"],
            "OrderStatus": orderStatus,
            "OrderDetail": consumedMessage["OrderDetails"],
            "OrderTime": consumedMessage["OrderTime"],
            "TotalCost": consumedMessage["TotalCost"]
        }

        print(
            f"Order {transactOrdersMessage['CustomerID']} has been processed!")

        ordersConfirmedProducer.send(
            KAFKA_TRANSACT_PRODUCER_TOPIC, transactOrdersMessage)
        # print(transactOrdersMessage)
