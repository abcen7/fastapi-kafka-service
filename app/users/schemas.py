from decimal import Decimal
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, field_serializer, field_validator


class UserCreate(BaseModel):
    username: str
    first_name: str
    last_name: str
    bio: str | None = None


# Data structures for Kafka


class CryptoServiceType(StrEnum):
    ETH = "ETH"
    BTC = "BTC"
    IOTA = "IOTA"


class UserResponse(BaseModel):
    id: int
    username: str
    first_name: str
    last_name: str
    bio: str | None = None


class UserDataRequest(BaseModel):
    correlation_id: UUID
    user_id: int

    @field_serializer("correlation_id")
    def serialize_correlation_id(self, correlation_id: UUID, _info):
        return str(correlation_id)


class UserDataResponse(UserDataRequest):
    produced_by: CryptoServiceType
    balance: Decimal

    @field_serializer("balance")
    def serialize_balance(self, balance: Decimal, _info):
        return float(balance)
