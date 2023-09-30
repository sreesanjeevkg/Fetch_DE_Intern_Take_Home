from kafka_helpers import KafkaHelper
import json
from confluent_kafka import Consumer
import logging

# Create an instance of KafkaHelper
kafka_helper = KafkaHelper()

realtime_logger = logging.getLogger('realtime-system')
kafka_helper.setup_logger(logger_level=logging.INFO, logger_filename='realtime.log', logger_instance=realtime_logger)

consumer_real_time = Consumer({'bootstrap.servers': 'localhost:9092'
                                  , 'group.id': 'consumer-realtime'
                                  , 'auto.offset.reset': 'earliest'
                                  , 'auto.commit.interval.ms': 1000
                                  , 'fetch.wait.max.ms': 20000})

realtime_logger.info('Available topics to consume: %s', consumer_real_time.list_topics().topics)

consumer_real_time.subscribe(['user-login'])
realtime_logger.info("Polling user-login topic")

def main():
    while True:
        msg = consumer_real_time.poll()
        if msg is None:
            realtime_logger.info('No message received')
            continue
        if msg.error():
            realtime_logger.info('Error: {}'.format(msg.error(), msg.topic(), msg.partition(), msg.offset()))
            continue
        data = msg.value().decode('utf-8')
        data = json.loads(data)
        ## TODO: Send it for realtime processing - like frontend or S3
        realtime_logger.info(data)

    consumer_real_time.close()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        realtime_logger.info('Consumer interrupted by user')
