from kafka_pack import IsConfirm
from constant import KAFKA_SERVERS, KAFKA_TOPICS


def ret(txid):
    return True, "ok"


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(
            msg.topic(), msg.partition()))


cc = IsConfirm(ret, KAFKA_SERVERS, "test", 2, delivery_report)
cc()
print(12)
cc.add_id_cache("18","198")

cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
cc.add_id_cache("19","199")
