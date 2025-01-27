import asyncio
import json
import time
import uuid
from typing import Sequence

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from ..core import settings
from ..core.lib import main_logger
from .models import User
from .repositories import UsersRepository
from .schemas import CryptoServiceType, UserCreate, UserDataRequest, UserDataResponse


class UsersService:
    repository = UsersRepository()

    async def create(self, user: UserCreate) -> None:
        return await self.repository.create(User(**user.model_dump()))

    async def get_all(self) -> Sequence[User]:
        return await self.repository.get_all()

    @classmethod
    async def _send_messages(cls, user_id: int, correlation_id: uuid.UUID) -> None:
        """Writes to balances_request topic, requesting user info"""
        try:
            async with AIOKafkaProducer(
                bootstrap_servers=settings.kafka.BROKER,
            ) as producer:
                main_logger.info("Started sending messages")
                request = UserDataRequest(
                    correlation_id=correlation_id,
                    user_id=user_id,
                )
                await producer.send(
                    value=json.dumps(request.model_dump()).encode("ascii"),
                    topic=settings.kafka.PRODUCE_TOPIC,
                )
                main_logger.info(f"Sent message to balances_request >> {request}")
                main_logger.info("Finished sending messages")
                return None
        except Exception as e:
            main_logger.error(f"Error in sending messages: {str(e)}")
            raise e

    async def get_all_info(self, user_id: int) -> float:
        count_waiting_messages = len(CryptoServiceType)
        threshold_time = 2.5
        balance = 0
        count_messages_received = 0
        start_time = time.time()
        request_correlation_id = uuid.uuid4()

        try:
            async with AIOKafkaConsumer(
                settings.kafka.CONSUME_TOPIC,
                bootstrap_servers=settings.kafka.BROKER,
            ) as consumer:
                await self._send_messages(user_id, request_correlation_id)

                while count_messages_received < count_waiting_messages:
                    if time.time() - start_time > threshold_time:
                        break

                    messages = await consumer.getmany(timeout_ms=0.1)

                    if not messages:
                        continue

                    for partition, message_list in messages.items():
                        for message in message_list:
                            decoded_message = UserDataResponse(
                                **json.loads(message.value)
                            )
                            if decoded_message.correlation_id == request_correlation_id:
                                main_logger.info(
                                    f"Received message from balances_response >> {decoded_message}"
                                )
                                balance += decoded_message.balance
                                count_messages_received += 1

                                if count_messages_received == count_waiting_messages:
                                    return balance

                return balance
        except Exception as e:
            main_logger.error(f"Error in consuming messages in get_all_info: {str(e)}")
            raise e
