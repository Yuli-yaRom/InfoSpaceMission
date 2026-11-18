from application.ports_out import EventPublisherPort
from domain.dtos import DomainEvent


# Видавець подій за замовчуванням: нічого не робить, тому основний потік не залежить від Kafka
class NoOpEventPublisher(EventPublisherPort):
    def publish(self, event: DomainEvent) -> None:
        pass
