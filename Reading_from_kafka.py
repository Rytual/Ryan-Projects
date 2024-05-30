from kafka import KafkaConsumer

# Kafka consumer parameters
topic_name = 'my_topic'
bootstrap_servers = ['localhost:9092']

# Create Kafka consumer
consumer = KafkaConsumer(topic_name, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest')

# Consume messages
for message in consumer:
    print(f"Received message: {message.value.decode('utf-8')}")
