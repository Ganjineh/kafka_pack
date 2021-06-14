from confluent_kafka import Consumer, KafkaError
from confluent_kafka import Producer
from flask import Flask, Response
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


class EndpointAction(object):

    def __init__(self, action, endpoint_name):
        self.action = action
        self.endpoint_name = endpoint_name
        self.response = Response(status=200, headers={})

    def __call__(self, address=None, *args):
        res = self.action(address)
        self.response.response = res
        return self.response


class FlaskAppWrapper(object):
    app = None

    def __init__(self, name, host='0.0.0.0', debug=False):
        self.app = Flask(name)
        self.host = host
        self.debug = debug

    def run(self):
        self.app.run(host=self.host, debug=self.debug)

    def add_endpoint(self, endpoint=None, endpoint_name=None, handler=None):
        self.app.add_url_rule(endpoint, endpoint_name,
                              EndpointAction(handler, endpoint_name))
