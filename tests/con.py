from kafka_pack import consumer
from confluent_kafka import KafkaError
import json
from constant import KAFKA_SERVERS, KAFKA_GROUPS, KAFKA_TOPICS

c = consumer(KAFKA_SERVERS, KAFKA_GROUPS, KAFKA_TOPICS)

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break
    print(msg.topic())
    message = msg.value().decode('utf-8')
    message = json.loads(message)
    print('Received message: {}'.format(message))
c.close()
