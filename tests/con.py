from kafka_pack import main
from confluent_kafka import KafkaError
import json

c = main.consumer('37.152.181.68:9092', 't1', '')

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
    # print(msg.topic())
    message = msg.value().decode('utf-8')
    message = json.loads(message)
    print('Received message: {}'.format(message))
    # 'doge_micro','eth_micro','tron_micro'
    if msg.topic() == 'doge_backend':
        main.producer('37.152.181.68:9092', 'doge_micro', {'id': message['id'],
                                                           'status_code': 500,
                                                           'msg': 'some bad things happened!'
                                                           })
    elif msg.topic() == 'eth_backend':
        main.producer('37.152.181.68:9092', 'eth_micro', {'id': message['id'],
                                                          'status_code': 500,
                                                          'msg': 'some bad things happened!'
                                                          })
    elif msg.topic() == 'tron_backend':
        main.producer('37.152.181.68:9092', 'tron_micro', {'id': message['id'],
                                                           'status_code': 500,
                                                           'msg': 'some bad things happened!'
                                                           })

c.close()
