from kafka_helpers import KafkaHelper
import json
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Consumer, Producer
import logging
from datetime import datetime

# Create an instance of KafkaHelper
kafka_helper = KafkaHelper()

olap_logger = logging.getLogger('olap-system')
kafka_helper.setup_logger(logger_level=logging.INFO, logger_filename='olap.log', logger_instance=olap_logger)

new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in ["processed_data"]]

required_fields = ['user_id', 'app_version', 'device_type', 'ip', 'locale', 'device_id', 'timestamp']

placeholder_values = {
    'user_id': 'missing_user_id',
    'app_version': 'x.x',
    'device_type': 'missing_device_type',
    'ip': 'missing_ip',
    'locale': 'missing_locale',
    'device_id': 'missing_device_id',
    'timestamp': 'missing_timestamp'
}

states = {
    "East": ["NY", "NJ", "PA", "MA", "CT", "RI", "VT", "NH", "ME", "DE", "MD", "VA", "WV", "NC", "SC", "GA", "FL"],
    "South": ["AL", "MS", "LA", "TX", "AR", "TN", "KY", "OK"],
    "Midwest": ["OH", "IN", "IL", "MI", "WI", "MN", "IA", "MO", "KS", "NE", "SD", "ND"],
    "West": ["CA", "OR", "WA", "NV", "AZ", "UT", "CO", "WY", "MT", "ID", "NM", "AK", "HI"]
}

consumer_olap = Consumer({'bootstrap.servers': 'localhost:9092'
                             , 'group.id': 'consumer-olap'
                             , 'auto.offset.reset': 'earliest'
                             , 'auto.commit.interval.ms': 1000
                             , 'fetch.wait.max.ms': 20000})

olap_logger.info('Available topics to consume: %s', consumer_olap.list_topics().topics)

consumer_olap.subscribe(['user-login'])
olap_logger.info("Polling user-login topic")


def process_data(data, states):
    hour = datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S').hour
    data = time_flg(data, hour)
    data = region(states, data)
    data_json = json.dumps(data)
    return data_json


def region(states, data):
    if data is not None:
        for region, states in states.items():
            if data["locale"] in states:
                data["region"] = region
                break
        if data["region"] is None:
            data["region"] = "Unknown"

    return data

def time_flg(data, hour):
    if 6 <= hour < 12:
        data["time_flg"] = 1
    elif 12 <= hour < 18:
        data["time_flg"] = 2
    elif 18 <= hour < 22:
        data["time_flg"] = 3
    else:
        data["time_flg"] = 4

    return data


def main():
    a = AdminClient({'bootstrap.servers': 'localhost:9092'})
    fs = a.create_topics(new_topics)
    kafka_helper.check_topic_creation_status(fs)

    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    while True:
        msg = consumer_olap.poll()
        if msg is None:
            olap_logger.info('No message received')
            continue
        if msg.error():
            olap_logger.info('Error: {}'.format(msg.error(), msg.topic(), msg.partition(), msg.offset()))
            continue
        data = msg.value().decode('utf-8')
        data = json.loads(data)
        # TODO: Do Cleaning, other transformation tasks
        if all(field in data for field in required_fields):
            data['timestamp'] = datetime.utcfromtimestamp(data['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
            data_json = process_data(data, states)
            producer.produce(topic='processed-data', value=data_json.encode('utf-8'))
        else:
            missing_fields = [field for field in required_fields if field not in data]
            olap_logger.info("Missing fields in data: %s", missing_fields)
            for missing_field in missing_fields:
                data[missing_field] = placeholder_values[missing_field] # Populating the missing fields with placeholder
            data['timestamp'] = datetime.utcfromtimestamp(data['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
            data_json = process_data(data, states)
            producer.produce(topic='processed-data', value=data_json.encode('utf-8'))

        olap_logger.info(data)

    consumer_olap.close()
    producer.flush()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        olap_logger.info('Consumer interrupted by user')
