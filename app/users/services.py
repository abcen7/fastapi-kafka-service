import asyncio
import json
import random
import time
import uuid
from decimal import Decimal
from typing import Sequence

from .models import User
from .repositories import UsersRepository
from .schemas import UserCreate, UserDataRequest, UserDataResponse, CryptoServiceType
from ..core import settings
from ..core.kafka.consumer import AIOWebConsumer
from ..core.kafka.producer import AIOWebProducer
from ..core.lib import main_logger

# TODO: add logging
class UsersService:
    repository = UsersRepository()

    async def __call__(self, *args, **kwargs):
        print("UserService object was just created")

    async def create(self, user: UserCreate) -> None:
        return await self.repository.create(User(**user.model_dump()))

    async def get_all(self) -> Sequence[User]:
        return await self.repository.get_all()


    async def simulate_process_messages(self) -> None:
        """
        Receives and processes messages from Kafka topic.
        """
        print('Started simulates processing messages')
        consumer = AIOWebConsumer(consume_topic=settings.kafka.PRODUCE_TOPIC)
        producer = AIOWebProducer(produce_topic=settings.kafka.CONSUME_TOPIC)
        await consumer.start()
        await producer.start()
        count_sent_messages = 0
        try:
            async for message in await consumer.get():
                decoded_message = json.loads(message.value)
                user_data_request = UserDataRequest(**decoded_message)
                print(f"Received message from balances_request >> {user_data_request}")
                for service_type in CryptoServiceType:
                    response = UserDataResponse(
                        correlation_id=user_data_request.correlation_id,
                        produced_by=service_type,
                        user_id=user_data_request.user_id,
                        balance=Decimal(random.uniform(1.0, 100.0))
                    )
                    await producer.send(json.dumps(response.model_dump()).encode("ascii"))
                    count_sent_messages += 1
                    print(f"Sent message to balances_response >> {response}")
                if count_sent_messages == len(CryptoServiceType):
                    print('Finished simulates processing messages')
                    return
        except Exception as e:
            print(f'Error in simulates processing messages: {str(e)}')
            raise e
        finally:
            await consumer.stop()
            await producer.stop()


    async def simulate_send_messages(self, user_id: int) -> None:
        """Writes to balances_request topic, requesting user info"""
        producer = AIOWebProducer()
        await producer.start()
        try:
            # TODO: Fix: warn to info (need to create the log config)
            main_logger.warn('Started simulates sending messages')
            request = UserDataRequest(
                correlation_id=uuid.uuid4(),
                user_id=user_id,
            )
            await producer.send(json.dumps(request.model_dump()).encode("ascii"))
            print(f"Sent message to balances_request >> {request}")
            main_logger.warn('Finished simulates sending messages')
            return None
        except Exception as e:
            main_logger.error(f'Error in simulates sending messages: {str(e)}')
            raise e
        finally:
            await producer.stop()

    async def get_all_info(self, user_id: int) -> float:
        """Consumes messages from balances_response"""
        # TODO: return async task interaction
        consumer = AIOWebConsumer()
        await consumer.start()

        count_waiting_messages = len(CryptoServiceType)
        threshold_time = 5
        balance = 0
        count_messages_received = 0
        start_time = time.time()

        process_task = asyncio.create_task(
            self.simulate_process_messages()
        )
        await self.simulate_send_messages(user_id)
        await process_task
        try:
            async for message in await consumer.get():
                decoded_message = UserDataResponse(**json.loads(message.value))
                print(f"Received message from balances_response >> {decoded_message}")
                balance += decoded_message.balance
                count_messages_received += 1
                if count_messages_received == count_waiting_messages:
                    return balance
                if time.time() - start_time > threshold_time:
                    return balance
        except Exception as e:
            main_logger.error(f'Error in consuming messages in get_all_info: {str(e)}')
            raise e
        finally:
            await consumer.stop()
