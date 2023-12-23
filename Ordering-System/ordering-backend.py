from faker import Faker
import json
import datetime
from time import sleep
import random
from kafka import KafkaProducer

KAFKA_TOPIC = "orders"
ORDER_LIMIT = 20

faker = Faker()

pizza_names = [
    "Margherita",
    "Marinara",
    "Diavola",
    "Mari & Monti",
    "Salami",
    "Peperoni",
]
pizza_toppings = [
    "🍅 tomato",
    "🧀 blue cheese",
    "🥚 egg",
    "🫑 green peppers",
    "🌶️ hot pepper",
    "🥓 bacon",
    "🫒 olives",
    "🧄 garlic",
    "🧅 onion",
    "🍗 Chicken"
]

orderProducer = KafkaProducer(
    bootstrap_servers=['3.110.81.255:9092'],
    value_serializer=lambda x: json.dumps(
        x, ensure_ascii=False).encode('utf-8')
)

print("Generating Orders.... \n")

for i in range(1, ORDER_LIMIT):
    quantity = random.randint(1, 5)
    print("Sending order " + str(i))
    message = {
        "OrderID": i,
        "Name": faker.name(),
        "Email": faker.email(),
        "Phone": faker.phone_number(),
        "Address": faker.address(),
        "OrderTime": str(datetime.datetime.now()),
        "OrderDetails":  random.choice(pizza_names) + " with " + random.choice(pizza_toppings) + " toppings.",
        "Quantity": quantity,
        "TotalCost": random.randint(150, 500) * quantity
    }

    orderProducer.send(KAFKA_TOPIC, message)
    sleep(2)
