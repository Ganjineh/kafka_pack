from kafka_pack import main
from confluent_kafka import KafkaError

c = main.consumer('37.152.181.68:9092', 't1', 'sample')

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

    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()
