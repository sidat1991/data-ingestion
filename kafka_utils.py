from typing import Dict, Any, Iterable
from confluent_kafka import Producer, Consumer
import socket
from configuration import KAFKA_BROKERS, KAFKA_TOPIC_NAME


def _get_producer_kafka_config() -> Dict[str, str]:
    return {
        "bootstrap.servers": ",".join(KAFKA_BROKERS),
        "client.id": socket.gethostname(),
    }

def _get_consumer_kafka_config() -> Dict[str, str]:
    return {
        "bootstrap.servers": ",".join(KAFKA_BROKERS),
        "client.id": socket.gethostname(),
        "group.id": "test",
        "auto.offset.reset": "earliest"
    }

class KafkaClient(object):
    def __init__(self):
        self.producer = Producer(_get_producer_kafka_config())
        self.consumer = Consumer(_get_consumer_kafka_config())


    def produce(self, key: str, value: Any) -> None:
        self.producer.produce(KAFKA_TOPIC_NAME, key=key, value=value)
        self.producer.flush()


    def consume(self) -> Iterable[Any]:
        try:
            self.consumer.subscribe([KAFKA_TOPIC_NAME])
            while True:
                message = self.consumer.poll(timeout=1.0)
                if message is None or message.error():
                    continue
                yield message.value()
        finally:
            self.consumer.close()
            yield None
