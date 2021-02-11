from kafka_pack import main

some_data_source = ["1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
                    "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1"]

for data in some_data_source:
    main.producer('37.152.181.68:9092', 'sample', data)
