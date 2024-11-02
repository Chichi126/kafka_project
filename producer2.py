from kafka import KafkaProducer
import time

# Initialize the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# List of messages
messages = [
    b"Hello from Docker Kafka producer: I love Game of Thrones",
    b"Kafka is a great tool for streaming",
    b"Message three in the sequence",
    b"Python and Kafka integration is powerful",
    b"Sending the fifth message"
]

# Send each message to the topic
for message in messages:
    producer.send('secondstream', message)
    print(f"Sent: {message}")
    time.sleep(1)  # Optional: to add a delay between sending messages

# Close the producer connection
producer.flush()
producer.close()
