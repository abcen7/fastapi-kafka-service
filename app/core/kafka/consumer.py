import asyncio
from pydoc_data.topics import topics
from typing import Literal, Union

from aiokafka import AIOKafkaConsumer

from app.core import settings


class AIOWebConsumer(object):
    def __init__(
            self,
            consume_topic: str = settings.kafka.CONSUME_TOPIC
    ):
        self.__consume_topic = consume_topic
        self._consumer = AIOKafkaConsumer(
            self.__consume_topic,
            bootstrap_servers=settings.kafka.BROKER,
            loop=asyncio.get_event_loop(),
        )

    async def get(self) -> AIOKafkaConsumer:
        return self._consumer

    async def start(self) -> None:
        await self._consumer.start()

    async def stop(self) -> None:
        await self._consumer.stop()
