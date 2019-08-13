import asyncio
import typing
from asyncio.tasks import ensure_future
from dataclasses import dataclass

import databases
import pydantic
import sqlalchemy
from cached_property import cached_property
from pydantic import BaseModel, EmailStr, SecretStr
from pydantic.main import MetaModel

from . import fields, queryset
from .queryset import QuerySet, CacheQuerySet
from .utils import get_field


@dataclass
class Field:
    name: str
    type: typing.Any


def create_db_column(field: Field, **kwargs) -> sqlalchemy.Column:
    field_name = field.name
    field_type = field.type_
    column = None
    if field_type == int:
        column = sqlalchemy.Integer
    elif field_type in [str, EmailStr, SecretStr]:
        length = kwargs.pop("length", None)
        column = sqlalchemy.String
        if length:
            column = sqlalchemy.String(length)
    elif field_type == bool:
        column = sqlalchemy.Boolean
    elif fields.is_json_field(field_type):
        jsonb = kwargs.pop("jsonb", None)
        column = sqlalchemy.JSON
        if jsonb:
            column = sqlalchemy.dialects.postgresql.JSONB
    elif field_type.__class__.__name__ in ["ModelMetaClass", "_GenericAlias"]:
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
        # This will never be called because the return type of `__new__` is wrong
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
    def model_fields(cls):
        return cls.__dict__["__fields__"]

    @classmethod
    def get_related_field_class(cls, key):
        if cls.is_related_field(key):
            klass_field = cls.get_class_field_from_db_name(key)
            return get_field(klass_field[1])
        return cls

    @classmethod
    def get_class_field_from_db_name(cls, db_name: str):
        fields = cls.model_fields()
        if db_name in list(fields.keys()):
            return db_name, fields[db_name].type_
        result = None
        for key, value in cls.Config.table_config.items():
            if value.get("name") == db_name:
                result = key, fields[key].type_
                break
        return result

    def _get_field_db_name(self, field: str) -> str:
        if self._is_related_field(field):
            return self.__class__.Config.table_config.get(field).get("name")
        return field

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

    async def save(self, using="default", connection=None):
        table = self.objects.table
        values = self.dict()
        clean_values = {}
        for key, value in values.items():
            if self._is_related_field(key):
                clean_values[self._get_field_db_name(key)] = value["id"]
            else:
                if type(value) == SecretStr:
                    clean_values[key] = value.display()
                else:
                    clean_values[key] = value
        if not self.id:
            sql = table.insert()
            clean_values.pop("id")
        else:
            sql = table.update()
        sql = sql.values(**clean_values)
        if self.id:
            sql = sql.where(table.c.id == self.id)
        # task = asyncio.create_task()
        result, _ = await asyncio.gather(
            self.databases[using].execute(query=sql),
            self.objects.remove_cache(self, connection=connection),
        )
        self.id = result
        # return task

    async def load(self):
        table = self.objects.table
        record = await self.objects.filter(id=self.id).get()
        self = record

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

    def db_dict(self):
        fields = self.__class__.model_fields()
        result = {}
        for key in fields.keys():
            value = getattr(self, key)
            name = self._get_field_db_name(key)
            if self._is_related_field(key):
                if value:
                    result[name] = value.id
                else:
                    result[name] = None
            else:
                result[name] = value
        return result

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
