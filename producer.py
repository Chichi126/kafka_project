from kafka import KafkaProducer
 
# Define the Kafka topic and bootstrap server
topic_name = 'secondstream'
bootstrap_servers = 'localhost:9092'  # 'kafka' is the service name defined in docker-compose
 
# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
 
# Message to send
messages = b"Hello from Docker Kafka producer: I love Game of Thrones"
    
 
# Send the message to the Kafka topic
producer.send(topic_name, messages)
 
# Flush the producer to ensure all messages are sent
producer.flush()
 
# Close the producer connection
producer.close()
 
print("Message sent successfully!")