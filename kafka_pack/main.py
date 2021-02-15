from confluent_kafka import Consumer, KafkaError
from confluent_kafka import Producer
import json


def test(s):
    print(s)


def consumer(servers, group, topic):
    c = Consumer({
        'bootstrap.servers': servers,
        'group.id': group,
        'default.topic.config': {
            'auto.offset.reset': 'smallest'
        }
    })
    c.subscribe(['doge_backend','eth_backend','tron_backend'])
    return c


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(
            msg.topic(), msg.partition()))


def producer(servers, topic, msg):
    p = Producer({'bootstrap.servers': servers})
    p.poll(0)
    p.produce(topic, json.dumps(msg), callback=delivery_report)
    p.flush()
