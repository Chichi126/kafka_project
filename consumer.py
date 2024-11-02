#Using the script below to read data from the kafka topic.
from kafka import KafkaConsumer
 
# Define the Kafka topic and bootstrap server
topic_name = 'secondstream'
bootstrap_servers = 'localhost:9092'  # Adjust as needed
 
# Initialize Kafka consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',  # Start reading from the beginning of the topic
    group_id='my-group'  # Consumer group ID (can be any string)
)
 
# Consume messages
print(f"Consuming messages from topic '{topic_name}'...")
for message in consumer:
    print(f"Received message: {message.value.decode('utf-8')}")