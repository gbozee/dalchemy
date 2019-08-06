from orm import Base, fields
from pydantic import EmailStr, SecretStr, validator
import typing


class User(Base):
    full_name: str
    email: EmailStr
    password: SecretStr = ""
    is_active: bool = True

    class Config:
        table_name = "users"
        table_config = {
            "id": {"primary_key": True, "index": True},
            "full_name": {"index": True},
            "email": {"unique": True},
            "is_active": {"default": True},
        }


class PhoneNumber(Base):
    number: str
    user: fields.Foreign(User)

    class Config:
        table_name = "phone_numbers"
        table_config = {
            "id": {"primary_key": True},
            "number": {"index": True, "length": 15},
            "user": {"name": "user_id", "type": int},
        }


class Profile(Base):
    addresses: typing.Optional[fields.JSON()] = []
    user: fields.Foreign(User)

    class Config:
        table_name = "profiles"
        table_config = {
            "id": {"primary_key": True},
            "user": {
                "name": "user_id",
                "type": int,
            },
            "addresses": {"jsonb": True},
        }

    @validator("addresses", pre=True, always=True)
    def set_addresses(cls, v):
        return v or []
