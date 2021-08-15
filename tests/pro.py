from kafka_pack import producer
from constant import KAFKA_SERVERS, KAFKA_TOPICS


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(48)
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(
            msg.topic(), msg.partition()))


some_data_source = ["1"] * 100

# for data in some_data_source:
producer("1818", KAFKA_TOPICS[0], "data", callback=delivery_report)
    # producer(KAFKA_SERVERS,  KAFKA_TOPICS[1], data, callback=delivery_report)
    # producer(KAFKA_SERVERS,  KAFKA_TOPICS[2], data, callback=delivery_report)
