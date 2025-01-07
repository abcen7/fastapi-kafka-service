import json
import random
import uuid
from decimal import Decimal
from typing import Sequence

from .models import User
from .repositories import UsersRepository
from .schemas import UserCreate, UserDataRequest, UserDataResponse, CryptoServiceType
from ..core.kafka.consumer import AIOWebConsumer
from ..core.kafka.producer import AIOWebProducer
from ..core.lib import main_logger


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
        finally:
            await producer.stop()

    async def simulate_process_messages(self) -> None:
        """
            Example action of microservice, receiving and processing messages from Kafka topic.
            Reads from balances_request topic and writes response to balances_response topic.
        """
        main_logger.warn('Started simulates processing messages')
        producer = AIOWebProducer()
        consumer = AIOWebConsumer()
        await consumer.start()
        await producer.start()
        try:
            print(1)
            async for message in await consumer.get():
                decoded_message = json.loads(message.value)
                print(decoded_message)
                user_data_request = UserDataRequest(**decoded_message)
                print(user_data_request)
                for service_type in CryptoServiceType:
                    response = UserDataResponse(
                        correlation_id=user_data_request.correlation_id,
                        produced_by=service_type,
                        user_id=user_data_request.user_id,
                        balance=Decimal(random.uniform(1.0, 100.0))
                    )
                    await producer.send(json.dumps(response.model_dump()).encode("ascii"))
                main_logger.warn('Finished simulates processing messages')
                return None
        except Exception as e:
            main_logger.error(f'Error in simulates processing messages: {str(e)}')
            raise e
        finally:
            await consumer.stop()
            await producer.stop()

    async def get_all_info(self, user_id: int) -> float:
        consumer = AIOWebConsumer()
        producer = AIOWebProducer()
        await consumer.start()
        await producer.start()
        await self.simulate_send_messages(user_id)
        await self.simulate_process_messages()
        balance = Decimal('0.0')
        try:
            async for message in await self.__consumer.get():
                decoded_message = UserDataResponse(**json.loads(message))
                print(decoded_message)
                balance += decoded_message.balance
            return float(balance)
        finally:
            await self.__consumer.stop()
            await self.__producer.stop()
