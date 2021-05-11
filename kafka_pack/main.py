from confluent_kafka import Consumer, KafkaError
from confluent_kafka import Producer
import json


def consumer(servers, group, topics):
    c = Consumer({
        'bootstrap.servers': servers,
        'group.id': group,
        'default.topic.config': {
            'auto.offset.reset': 'smallest'
        }
    })
    t = []
    for i in topics:
        t.append(i)
    c.subscribe(t)
    return c


def producer(servers, topic, msg, callback):
    p = Producer({'bootstrap.servers': servers})
    p.poll(0)
    p.produce(topic, json.dumps(msg), callback=callback)
    p.flush()
