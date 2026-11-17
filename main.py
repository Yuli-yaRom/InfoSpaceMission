import sys
from pathlib import Path

# Додаємо корінь проєкту в системні шляхи Python, щоб не було ModuleNotFoundError
sys.path.append(str(Path(__file__).resolve().parent))

from adapters.inbound.controllers import GeneratorIngestController
from application.ports_out import EventPublisherPort
from application.services import MissionService
from domain.dtos import BatchIngestRequest, DomainEvent, IngestMissionItemDTO


# 1. Створюємо mock-адаптер для Kafka, щоб не запускати справжній сервер
class MockKafkaAdapter(EventPublisherPort):

  def publish(self, event: DomainEvent) -> None:
    print(f" [Kafka Mock] Подію успішно опубліковано: {event}")


def main():
  print("--- Запуск тестового складання системи (Dependency Injection) ---")

  # 2. Збираємо ланцюжок залежностей:
  # Адаптер Kafka -> сервіс бізнес-логіки
  kafka_mock = MockKafkaAdapter()
  mission_service = MissionService(event_publisher=kafka_mock)

  # Сервіс -> вхідний контролер
  controller = GeneratorIngestController(create_mission_use_case=mission_service)

  # 3. Формуємо тестовий пакет місій від генератора
  test_request = BatchIngestRequest(
      batch_id="batch-2026-test",
      items=[
          IngestMissionItemDTO(
              name="Artemis III",
              agency_id="agency-nasa",
              celestial_body_id="moon",
              mission_type="LUNAR",
              description="Пілотована місія висадки на Місяць",
          ),
          IngestMissionItemDTO(
              name="Perseverance Rover",
              agency_id="agency-nasa",
              celestial_body_id="mars",
              mission_type="ROVER",
              description="Дослідження кратера Єзеро на Марсі",
          ),
      ],
  )

  # 4. Викликаємо контролер
  print("\nНадсилаємо пачку місій у контролер...")
  response = controller.handle_batch_ingest(test_request)

  print("\n--- Результат відповіді контролера ---")
  print(f"Batch ID: {response.batch_id}")
  print(f"Опрацьовано: {response.total_received} місій")
  print(f"Статус: {response.status}")
  print(f"Повідомлення: {response.message}")
  print("--- Тест успішно пройдено! ---")


if __name__ == "__main__":
  main()