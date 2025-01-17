import asyncio
import json
import random
from decimal import Decimal

from app.core import settings
from app.core.kafka.consumer import AIOWebConsumer
from app.core.kafka.producer import AIOWebProducer
from app.core.lib import main_logger
from app.users.schemas import UserDataRequest, UserDataResponse, CryptoServiceType


async def simulate_process_messages() -> None:
    """
    Receives and processes messages from Kafka topic.
    """
    main_logger.info('Started simulates processing messages')
    consumer = AIOWebConsumer(consume_topic=settings.kafka.PRODUCE_TOPIC)
    producer = AIOWebProducer(produce_topic=settings.kafka.CONSUME_TOPIC)
    await consumer.start()
    await producer.start()
    try:
        async for message in await consumer.get():
            decoded_message = json.loads(message.value)
            main_logger.info(f'Decoded message: {decoded_message}')
            user_data_request = UserDataRequest(**decoded_message)
            for service_type in CryptoServiceType:
                response = UserDataResponse(
                    correlation_id=user_data_request.correlation_id,
                    produced_by=service_type,
                    user_id=user_data_request.user_id,
                    balance=Decimal(random.uniform(1.0, 100.0))
                )
                main_logger.info(f'Response: {response}')
                try:
                    await producer.send(json.dumps(response.model_dump()).encode("ascii"))
                except Exception as send_error:
                    main_logger.error(f"Error sending message: {send_error}")
                main_logger("The message was sent successfully")
        main_logger.info('Finished simulates processing messages')
        return None
    except Exception as e:
        main_logger.error(f'Error in simulates processing messages: {str(e)}')
        raise e
    finally:
        await consumer.stop()
        await producer.stop()


def main():
    asyncio.run(simulate_process_messages())


if __name__ == '__main__':
    main()
