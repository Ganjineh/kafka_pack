from confluent_kafka import Consumer, KafkaError
from confluent_kafka import Producer
from flask import Flask, jsonify, request
from .handlers import set_in_cache, get_from_cache, del_from_cache, is_in_cache
from requests.exceptions import ConnectTimeout
import json
import time
from threading import Thread


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
    try:
        re = p.flush(timeout=10)
        if re > 0:
            raise ConnectTimeout
    except:
        raise ConnectTimeout


class EndpointAction(object):

    def __init__(self, action, endpoint_name):
        self.action = action
        self.endpoint_name = endpoint_name

    def __call__(self, address=None, *args):
        token = request.args.get('token')
        to = request.args.get('to')
        if to is not None:
            fee = request.args.get('fee')
            amount = request.args.get('amount')
            res = self.action(address, to, amount, fee)
        elif token is None:
            res = self.action(address)
        else:
            res = self.action(address, token)
        return jsonify(res)


class FlaskAppWrapper(object):
    app = None

    def __init__(self, name, host='0.0.0.0', debug=False, port=5000):
        self.app = Flask(name)
        self.host = host
        self.debug = debug
        self.port = port

    def run(self):
        self.app.run(host=self.host, debug=self.debug, port=self.port)

    def add_endpoint(self, endpoint=None, endpoint_name=None, handler=None):
        self.app.add_url_rule(endpoint, endpoint_name,
                              EndpointAction(handler, endpoint_name))


class IsConfirm(object):
    def __init__(self, action, kafka_servers, topics, block_time, callback):
        self.action = action
        self.kafka_servers = kafka_servers
        self.topics = topics
        self.block_time = block_time
        self.callback = callback

    def add_id_cache(self, txid, callback_id):
        val = get_from_cache(str(self.topics[:15])+'_pendding')
        if len(val) > 0:
            val += "|"
        val += str(txid)+","+str(callback_id)
        set_in_cache(str(self.topics[:15])+'_pendding',
                     val, 60*60)

    def __call__(self):
        thread = Thread(target=self.checking)
        thread.start()

    def checking(self):
        while True:
            try:
                val = get_from_cache(str(self.topics[:15])+'_pendding')
                set_in_cache(str(self.topics[:15])+'_pendding',
                             '', 60*60)
                for i in str(val).split("|"):
                    if len(str(i)) == 0:
                        continue
                    i = i.split(",")
                    res = self.action(str(i[0]))
                    if res[0]:
                        ret = {
                            'txid': str(i[0]),
                            'callback_id':  str(i[1]),
                            'status': res[1]
                        }
                        producer(self.kafka_servers,
                                 self.topics, ret, self.callback)
                    else:
                        self.add_id_cache(str(i[0]), str(i[1]))

                print("running...")
                time.sleep(int(self.block_time))
            except Exception as e:
                print("error: "+str(e))
                time.sleep(1)
