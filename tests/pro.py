from kafka_pack import main

some_data_source = ["1"] * 100

for data in some_data_source:
    # 'doge_backend','eth_backend','tron_backend'
    main.producer('37.152.181.68:9092', 'tron_backend', data)
    main.producer('37.152.181.68:9092', 'eth_backend', data)
    main.producer('37.152.181.68:9092', 'doge_backend', data)
