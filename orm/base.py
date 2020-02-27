import asyncio
import datetime
import enum
import logging
import typing
from asyncio.tasks import ensure_future
from dataclasses import dataclass

import asyncpg
import databases
import pydantic
import sqlalchemy
from cached_property import cached_property
from pydantic import BaseModel, EmailStr, SecretStr
from pydantic.main import ModelMetaclass as MetaModel

from . import exceptions, fields, queryset
from .queryset import CacheQuerySet, QuerySet
from .utils import get_field


@dataclass
class Field:
    name: str
    type: typing.Any


def create_db_column(field: Field, **kwargs) -> sqlalchemy.Column:
    field_name = field.name
    field_type = field.type_
    column = None
    # import ipdb; ipdb.set_trace()
    if field_type == int:
        column = sqlalchemy.Integer
    elif field_type == float:
        column = sqlalchemy.Float
    elif hasattr(field_type, "mro") and enum.Enum in field_type.mro():
        column = sqlalchemy.Enum(field_type)

    elif field_type in [str, EmailStr, SecretStr]:
        length = kwargs.pop("length", None)
        column = sqlalchemy.String
        if length:
            column = sqlalchemy.String(length)
    elif field_type == bool:
        column = sqlalchemy.Boolean
    elif field_type == datetime.datetime:
        column = sqlalchemy.DateTime
        postgres = kwargs.pop("timestamp", None)
        # if postgres:
        #     column = sqlalchemy.dialects.postgresql.TIMESTAMP
    elif field_type == datetime.date:
        column = sqlalchemy.Date
    elif fields.is_json_field(field_type):
        jsonb = kwargs.pop("jsonb", None)
        column = sqlalchemy.JSON
        if jsonb:
            column = sqlalchemy.dialects.postgresql.JSONB
            # kwargs.update(astext_type=sqlalchemy.String)
    elif field_type.__class__.__name__ in ["ModelMetaClass", "_GenericAlias", "_Union"]:
        # is a foreign key
        field_value = get_field(field_type)
        name = kwargs.pop("name")
        _field_type = kwargs.pop("type")
        default_field_column = sqlalchemy.Integer
        if _field_type == str:
            default_field_column = sqlalchemy.String

        fk_name = f"{field_value.Config.table_name}.id"
        fk_kwargs = kwargs.pop("fk_kwargs", None) or {
            "onupdate": "CASCADE",
            "ondelete": "CASCADE",
        }
        return sqlalchemy.Column(
            name,
            default_field_column,
            sqlalchemy.ForeignKey(fk_name, **fk_kwargs),
            **kwargs,
        )
    if column:
        return sqlalchemy.Column(field_name, column, **kwargs)


class ModelMetaClass(MetaModel):
    def __init__(self, name, bases, namespace, **kwargs):
        # This will never be called because the return type of `__new__` is 
        super().__init__(name, bases, namespace, **kwargs)
        self.objects = queryset.QuerySet(self)

    @property
    def table(self) -> sqlalchemy.Table:
        return self.objects.table

    @property
    def c(self):
        return self.table.c


class Base(BaseModel, metaclass=ModelMetaClass):
    id: int = 0

    def _is_related_field(self, field: str) -> bool:
        return self.__class__.is_related_field(field)

    @classmethod
    def is_json_field(cls, field: str) -> bool:
        type_class = cls.model_fields()[field].type_
        return fields.is_json_field(type_class)

    @classmethod
    def is_related_field(cls, field: str) -> bool:
        type_class = cls.model_fields()[field].type_
        return fields.is_related_field(type_class)

    @classmethod
    def model_fields(cls) -> dict:
        return cls.__dict__["__fields__"]

    @classmethod
    def get_related_field_class(cls, key):
        if cls.is_related_field(key):
            klass_field = cls.get_class_field_from_db_name(key)
            return get_field(klass_field[1])
        return cls

    @classmethod
    async def transform_kwargs(cls, **kwargs):
        """If a foreign key `id` field is passed instead of the foreign key field,
        the info is fetched and the foreign key field is used
        """
        model_fields = cls.model_fields().keys()
        non_model_fields = set(kwargs.keys()).difference(set(model_fields))
        if non_model_fields:
            # check if any of the field is a related field instead
            for key in non_model_fields:
                id_value = kwargs.pop(key)
                result = cls.get_class_field_from_db_name(key)
                if result:
                    new_key = result[0]
                    new_key_class = cls.get_related_field_class(new_key)
                    instance = await new_key_class.objects.get(id=id_value)
                    kwargs[new_key] = instance
        return kwargs

    @classmethod
    def get_class_field_from_db_name(
        cls, db_name: str
    ) -> typing.Tuple[str, typing.Any]:
        fields = cls.model_fields()
        if db_name in list(fields.keys()):
            return db_name, fields[db_name].type_
        result = None
        for key, value in cls.Config.table_config.items():
            if value.get("name") == db_name:
                result = key, fields[key].type_
                break
        return result

    @classmethod
    def with_defaults(cls, kwargs):
        """Sets default fields in addition to passed fields"""
        new_kwargs = cls.update_passed_values(kwargs)
        kwargs.update(new_kwargs)
        return kwargs

    @classmethod
    def validate_model(cls, **kwargs):
        try:
            new_kwargs = cls.with_defaults(kwargs)
            instance = cls(**new_kwargs)
        except pydantic.ValidationError as e:
            result = {}
            for value in e.errors():
                field = value["loc"][0]
                existing_value = result.get(field) or []
                existing_value.append(value["type"])
                result[field] = existing_value
            return result
        else:
            return None

    @classmethod
    def get_field_db_name(cls, field: str):
        if cls.is_related_field(field):
            return cls.Config.table_config.get(field).get("name")
        return field

    def _get_field_db_name(self, field: str) -> str:
        return self.__class__.get_field_db_name(field)

    @classmethod
    def build_table(cls, metadata: sqlalchemy.MetaData) -> sqlalchemy.Table:
        columns = []
        table_name = cls.Config.table_name
        config = cls.Config.table_config or {}
        for key, value in cls.model_fields().items():
            if key not in ["table_config", "table_name"]:
                attr = config.get(key) or {}
                columns.append(create_db_column(value, **attr))
        return sqlalchemy.Table(table_name, metadata, *columns, extend_existing=True)

    async def quick_save(self):
        if hasattr(self.__class__, "redis_conn"):
            redis_conn = await self.__class__.redis_conn()

    async def delete(self):
        await self.objects.filter(id=self.id).delete()
        return None

    def build_db_save_params(self):
        values = self.dict()
        clean_values = {}
        for key, value in values.items():
            if self._is_related_field(key):
                if value:
                    clean_values[self._get_field_db_name(key)] = value["id"]
            else:
                if type(value) == SecretStr:
                    clean_values[key] = value.get_secret_value()
                else:
                    clean_values[key] = value

        return clean_values

    @classmethod
    def update_passed_values(cls, clean_values):
        table = cls.table

        def get_default_values(o, passed_value):
            default = cls.Config.table_config[o].get("default")
            onupdate = cls.Config.table_config[o].get("onupdate")
            if onupdate:
                value = onupdate()
                if type(value) == datetime.datetime:
                    return value.replace(tzinfo=None)
                return value
            if callable(default):
                value = passed_value or default()
                if type(value) == datetime.datetime:
                    return value.replace(tzinfo=None)
                return value
            if passed_value == False:
                return passed_value
            value = passed_value or default
            if type(value) == datetime.datetime:
                return value.replace(tzinfo=None)
            return value

        default_values = {
            key: get_default_values(key, clean_values.get(key))
            for key, value in cls.Config.table_config.items()
            if any([value.get("default"), value.get("onupdate")])
        }
        return default_values

    @classmethod
    async def save_model(cls, clean_values, _id=0, using="default", connection=None):
        table = cls.table
        obj = None
        if _id:
            # create class instance when no id exists
            obj = cls(**clean_values)
        if not _id:
            sql = table.insert()
            clean_values.pop("id")
        else:
            sql = table.update()
        sql = sql.values(**clean_values)
        if _id:
            sql = sql.where(table.c.id == _id)
        # task = asyncio.create_task()
        # import ipdb; ipdb.set_trace()
        tasks = [cls.databases[using].execute(query=sql)]
        if connection:
            if not obj:
                obj = cls(**clean_values)
        if obj:
            tasks.append(cls.objects.remove_cache(obj, connection=connection))

        result = await asyncio.gather(*tasks)

        return result[0]

    async def save(self, using="default", connection=None):
        clean_values = self.build_db_save_params()
        with_default_values = self.__class__.update_passed_values(clean_values)
        try:
            result = await self.__class__.save_model(
                {**clean_values, **with_default_values},
                # clean_values,
                _id=self.id,
                using=using,
                connection=connection,
            )
            if result:
                self.id = result
            for key, value in with_default_values.items():
                setattr(self, key, value)
        except asyncpg.exceptions.InterfaceError as e:
            await self.databases[using].disconnect()
            raise

        # return task

    async def load(self):
        table = self.objects.table
        record = await self.objects.filter(id=self.id).get()
        self = record

    def as_dict(self):
        result = {}
        for key, value in self.dict().items():
            if type(value) == SecretStr:
                result[key] = value.get_secret_value()
            else:
                result[key] = value
        return result

    @classmethod
    def init_db_params(
        cls,
        database: typing.Dict[str, databases.Database],
        metadata: sqlalchemy.MetaData,
        redis_instance=None,
    ):
        cls.databases = database
        cls.metadata = metadata
        cls.database = database.get("default")
        if redis_instance:
            cls.redis_conn = redis_instance

    def __eq__(self, value):
        return self.id == value.id

    @classmethod
    def build_db_dict(cls, instance):
        fields = cls.model_fields()
        result = {}
        non_model_fields = {}
        if isinstance(instance, dict):
            non_model_fields = set(instance.keys()).difference(fields.keys())
        for key in fields.keys():
            if isinstance(instance, dict):
                value = instance.get(key)
            else:
                value = getattr(instance, key)
            name = cls.get_field_db_name(key)
            if cls.is_related_field(key):
                if value:
                    result[name] = value.id
                else:

                    result[name] = None
            else:
                result[name] = value
        # add the remaining non model fields to the db
        for j in non_model_fields:
            result[j] = instance.get(j)
        return result

    def db_dict(self):
        fields = self.__class__.model_fields()
        return self.__class__.build_db_dict(self)
        # result = {}
        # for key in fields.keys():
        #     value = getattr(self, key)
        #     name = self._get_field_db_name(key)
        #     if self._is_related_field(key):
        #         if value:
        #             result[name] = value.id
        #         else:
        #             result[name] = None
        #     else:
        #         result[name] = value
        # return result

    async def in_write_cache(self, connection=None) -> bool:
        return await self.objects.in_write_cache(self, connection=connection)


class CacheMetaClass(MetaModel):
    def __init__(self, name, bases, namespace, **kwargs):
        # This will never be called because the return type of `__new__` is wrong
        super().__init__(name, bases, namespace, **kwargs)
        self.objects = CacheQuerySet(self)


class CacheBase(BaseModel, metaclass=CacheMetaClass):
    from_db: bool = True

    @classmethod
    def model_fields(cls):
        return cls.__dict__["__fields__"]

    @property
    def cache_id(self):
        table = self.__class__.Config.cache_key
        field = self.__class__.Config.cache_field
        return f"{table}:{getattr(self, field)}"

    async def update_cache(self, connection):
        await self.objects.save_to_cache(self, connection)

    def items(self):
        return self.dict().items()
