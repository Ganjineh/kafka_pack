from kafka_pack import FlaskAppWrapper


def balance(address, token):
    return 'balance'+token


def transaction(address):
    return 'balance'


a = FlaskAppWrapper(__name__, debug=True, port=5000)
a.add_endpoint(endpoint='/balance/<address>',
               endpoint_name='balance', handler=balance)
a.add_endpoint(endpoint='/transaction/<address>',
               endpoint_name='transaction', handler=transaction)
a.run()
