import typing
from datetime import datetime

import sqlalchemy
from pydantic import EmailStr, SecretStr, validator

from orm import Base, CacheBase, fields


class User(Base):
    full_name: str
    email: EmailStr
    password: SecretStr = ""
    is_active: bool = True
    created: datetime = None
    modified: datetime = None

    class Config:
        table_name = "users"
        cache_field = "email"
        table_config = {
            "id": {"primary_key": True, "index": True},
            "full_name": {"index": True},
            "email": {"unique": True},
            "is_active": {"default": True},
            "created": {"default": datetime.now},
            "modified": {"onupdate": datetime.now},
        }


class Skill(Base):
    name: str

    class Config:
        table_name = "skills"
        cache_field = "name"
        table_config = {"id": {"primary_key": True}}


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
            "user": {"name": "user_id", "type": int},
            "addresses": {"jsonb": True},
        }

    @validator("addresses", pre=True, always=True)
    def set_addresses(cls, v):
        return v or []


class UserInfo(CacheBase):
    full_name: str
    email: EmailStr
    is_active: bool = True
    addresses: typing.List[dict] = []
    numbers: typing.List[str] = []

    class Config:
        cache_key = "user_info"
        cache_field = "email"

    @classmethod
    async def get_data(cls, key):
        profile = await Profile.objects.filter(user__email=key).get()
        phone_numbers = await PhoneNumber.objects.filter(user__email=key).all()
        user = profile.user
        return dict(
            full_name=user.full_name,
            email=user.email,
            is_active=user.is_active,
            addresses=profile.addresses,
            numbers=[x.number for x in phone_numbers],
        )


class ProfileCache(CacheBase):
    email: EmailStr
    user: User

    class Config:
        cache_key = "profile_info"
        cache_field = "email"

    @classmethod
    async def get_data(cls, key):
        user = await User.objects.filter(email=key).get()
        return dict(email=user.email, user=user)
