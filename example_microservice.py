import asyncio
import json
import random
from decimal import Decimal

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.core import settings
from app.core.lib import main_logger
from app.users.schemas import CryptoServiceType, UserDataRequest, UserDataResponse


async def simulate_process_messages() -> None:
    """Receives and processes messages from Kafka topic."""
    main_logger.info("Started simulates processing messages")
    try:
        async with AIOKafkaConsumer(
            settings.kafka.PRODUCE_TOPIC,
            bootstrap_servers=settings.kafka.BROKER,
        ) as consumer:
            async with AIOKafkaProducer(
                bootstrap_servers=settings.kafka.BROKER,
            ) as producer:
                async for message in consumer:
                    decoded_message = json.loads(message.value)
                    main_logger.info(f"Decoded message: {decoded_message}")
                    user_data_request = UserDataRequest(**decoded_message)
                    for service_type in CryptoServiceType:
                        response = UserDataResponse(
                            correlation_id=user_data_request.correlation_id,
                            produced_by=service_type,
                            user_id=user_data_request.user_id,
                            balance=Decimal(random.uniform(1.0, 100.0)),
                        )
                        main_logger.info(f"Response: {response}")
                        await producer.send(
                            value=json.dumps(response.model_dump()).encode("ascii"),
                            topic=settings.kafka.CONSUME_TOPIC,
                        )
                        main_logger.info("The message was sent successfully")
                main_logger.info("Finished simulates processing messages")
                return None
    except Exception as e:
        main_logger.error(f"Error in simulates processing messages: {str(e)}")
        raise e


def main():
    asyncio.run(simulate_process_messages())


if __name__ == "__main__":
    main()
