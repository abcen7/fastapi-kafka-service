import asyncio
import json
import time
import uuid
from itertools import count
from typing import Sequence

from .models import User
from .repositories import UsersRepository
from .schemas import UserCreate, UserDataRequest, UserDataResponse, CryptoServiceType
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

    async def simulate_send_messages(self, user_id: int) -> None:
        """Writes to balances_request topic, requesting user info"""
        producer = AIOWebProducer()
        await producer.start()
        try:
            main_logger.warn('Started simulates sending messages')
            request = UserDataRequest(
                correlation_id=uuid.uuid4(),
                user_id=user_id,
            )
            await producer.send(json.dumps(request.model_dump()).encode("ascii"))
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
        # Затем отправляем сообщение
        await self.simulate_send_messages(user_id)
        balance = 0
        count_messages_received = 0
        start_time = time.time()
        try:
            async for message in await consumer.get():
                decoded_message = UserDataResponse(**json.loads(message.value))
                balance += decoded_message.balance
                count_messages_received += 1
                if time.time() - start_time > 10:
                    return balance
            print(balance)
        except Exception as e:
            main_logger.error(f'Error in consuming messages in get_all_info: {str(e)}')
            raise e
        finally:
            await consumer.stop()
