from application.ports_out import EventPublisherPort
from domain.dtos import DomainEvent


# Адаптер для серіалізації та публікації повідомлень у брокер Kafka
# Вмикається лише для великих обсягів текстів (за зауваженням викладачки); за замовчуванням використовується NoOpEventPublisher
class KafkaEventPublisherAdapter(EventPublisherPort):
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers

    def publish(self, event: DomainEvent) -> None:
        raise NotImplementedError("TODO: Implement Kafka publish event")