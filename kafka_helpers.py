# kafka_helpers.py

import json
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
import logging


class KafkaHelper:
    def setup_logger(self, logger_level, logger_filename, logger_instance):
        logger_instance.setLevel(logger_level)

        filename = logger_filename
        handler = logging.FileHandler(filename)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger_instance.addHandler(handler)

    def configure_admin_client(self, bootstrap_servers):
        self.admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    def delivery_report(self, errmsg, msg):
        if errmsg is not None:
            print("Delivery failed for Message: {} : {}".format(msg.key(), errmsg))
            return
        print('Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}'.format(
                    msg.key(), msg.topic(), msg.partition(), msg.offset()))

    def check_topic_creation_status(self, topic_futures):
        for topic, future in topic_futures.items():
            try:
                future.result()
                print("Topic {} created".format(topic))
            except Exception as e:
                print("Failed to create topic {}: {}".format(topic, e))
