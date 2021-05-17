import threading
import logging
import time
import json
from kafka import KafkaConsumer, KafkaProducer

#https://medium.com/@mukeshkumar_46704/consume-json-messages-from-kafka-using-kafka-pythons-deserializer-859f5d39e02c

class Producer(threading.Thread):
    daemon = True
    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:8088',
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                 api_version = (0, 10, 1),
                                 )
        while True:
            producer.send('my-topic', {"dataObjectID": "test1"})
            producer.send('my-topic', {"dataObjectID": "test2"})
            time.sleep(1)


class Consumer(threading.Thread):
    daemon = True
    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:8088',
                                 auto_offset_reset='earliest',
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        consumer.subscribe(['my-topic'])
        for message in consumer:
            print (message)


def main():
    threads = [
        Producer(),
        Consumer()
    ]
    for t in threads:
        t.start()
    time.sleep(10)
if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:' +
               '%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()