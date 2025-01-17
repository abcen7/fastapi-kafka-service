import asyncio

from aiokafka import AIOKafkaProducer

from app.core import settings


class AIOWebProducer(object):
    def __init__(self, produce_topic: str = settings.kafka.PRODUCE_TOPIC):
        self.__producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka.BROKER,
            loop=asyncio.get_event_loop(),
        )
        self.__produce_topic = produce_topic

    async def start(self) -> None:
        await self.__producer.start()

    async def stop(self) -> None:
        await self.__producer.stop()

    async def send(self, value: bytes) -> None:
        await self.__producer.send(
            topic=self.__produce_topic,
            value=value,
        )
