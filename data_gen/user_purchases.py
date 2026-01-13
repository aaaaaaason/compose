import json
import time
import random
from confluent_kafka import Producer
from faker import Faker

# Initialize Faker
fake = Faker()

# Kafka Configuration
conf = {
    "bootstrap.servers": "localhost:9094,localhost:9095,localhost:9096",  # Change to your broker address
    "client.id": "fake-user-purchases-producer",
}

# Create Producer instance
producer = Producer(conf)


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def generate_fake_data():
    """Creates a dictionary of fake purchase data."""
    return {
        "user_id": fake.uuid4(),
        "user_name": fake.name(),
        "item": random.choice(["Laptop", "Phone", "Headphones", "Monitor", "Keyboard"]),
        "price": round(random.uniform(10.0, 1500.0), 2),
        "timestamp": fake.iso8601(),
        "country": fake.country(),
    }


# Main Loop
topic_name = "user_purchases"

try:
    print(f"Starting producer. Sending data to topic: {topic_name}")
    while True:
        data = generate_fake_data()

        # Trigger any available delivery report callbacks from previous produce() calls
        producer.poll(0)

        # Asynchronously send data (encoded as JSON)
        producer.produce(
            topic_name,
            key=data["user_id"],
            value=json.dumps(data).encode("utf-8"),
            callback=delivery_report,
        )

        time.sleep(1)  # Wait 1 second between messages

except KeyboardInterrupt:
    print("\nStopping producer...")

finally:
    # Wait for any outstanding messages to be delivered
    producer.flush()
