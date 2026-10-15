from command_service.ports.output import EventPublisherPort


class KafkaEventPublisherAdapter(EventPublisherPort):
    def __init__(self, kafka_producer=None):
        self._kafka_producer = kafka_producer  # TODO: підключення до Kafka

    def publish(self, topic: str, payload: dict) -> None:
        # TODO: надіслати повідомлення в Kafka-топік
        raise NotImplementedError