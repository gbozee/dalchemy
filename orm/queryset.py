import typing
from dataclasses import dataclass
import json
import asyncio
import aioredis
import databases
import sqlalchemy
import datetime
from cached_property import cached_property
from pydantic import SecretStr
from . import utils, exceptions
from .fields import CustomField
import logging

logging.basicConfig(level=logging.INFO)


@dataclass
class QuerySetParam:
    metadata: sqlalchemy.MetaData
    database: databases.Database


FILTER_OPERATORS = {
    "exact": "__eq__",
    "iexact": "ilike",
    "contains": "like",
    "icontains": "ilike",
    "in": "in_",
    "gt": "__gt__",
    "gte": "__ge__",
    "lt": "__lt__",
    "lte": "__le__",
}

JSON_OPERATORS = {
    "contained_by": "contained_by",
    "contains": "contains",
    "icontains": "contains",
    "has_all": "has_all",
    "has_any": "has_any",
    "has_key": "has_key",
}


def to_redis_dict(dictionary: dict, exclude: typing.List[str] = None):
    result: typing.Dict[str, typing.Any] = {}
    for key, value in dictionary.items():
        if type(value) == bool:
            result[key] = int(value)
        elif type(value) in [list, dict]:
            result[key] = json.dumps(value)
        elif type(value) in [datetime.datetime, datetime.date]:
            result[key] = value.timestamp()
        elif type(value) == SecretStr:
            result[key] = value.get_secret_value()
        else:
            if exclude:
                if key not in exclude:
                    result[key] = value
            else:
                result[key] = value
    return result


def to_python_dict(redis_dict: dict, class_fields: dict):
    actual_dict = {}
    for key, value in class_fields.items():
        if value.type_ == bool:
            actual_dict[key] = bool(redis_dict[key])
        elif (value.type_ in [dict, list] or value.default in [[], {}]) and type(
            redis_dict[key]
        ) == str:
            actual_dict[key] = json.loads(redis_dict[key])
        else:
            actual_dict[key] = redis_dict[key]
    return actual_dict


class QuerySetMixin:
    def dict_to_redis_dict(self, dictionary: dict, exclude: typing.List[str] = None):
        from orm.base import Base

        result: typing.Dict[str, typing.Any] = {}
        for key, value in dictionary.items():
            if type(value) == bool:
                result[key] = int(value)
            elif type(value) == list:
                result[key] = json.dumps(value)
            elif type(value) == dict:
                result[key] = json.dumps(to_redis_dict(value))
            elif type(value) in [datetime.datetime, datetime.date]:
                result[key] = value.timestamp()
            elif isinstance(value, Base):
                result[key] = json.dumps(to_redis_dict(value.as_dict()))
            else:
                if exclude:
                    if key not in exclude:
                        result[key] = value
                else:
                    result[key] = value
        return result

    def obj_to_redis_dict(self, obj, class_fields, exclude=None):
        result = {}
        for key, value in class_fields.items():
            if value.type_ == bool:
                result[key] = int(getattr(obj, key))
            elif value.type_ in [datetime.datetime, datetime.date]:
                result[key] = getattr(obj, key).timestamp()
            else:
                if exclude:
                    if key not in exclude:
                        result[key] = getattr(obj, key)
                else:
                    result[key] = getattr(obj, key)
        return result

    def redis_dict_to_obj(self, as_dict: dict, class_fields, klass):
        actual_dict = {}
        from orm.base import Base

        for key, value in class_fields.items():

            if key != "id":
                if value.type_ == bool:
                    actual_dict[key] = bool(as_dict[key])
                elif (value.type_ in [dict, list] or value.default == []) and type(
                    as_dict[key]
                ) == str:
                    actual_dict[key] = json.loads(as_dict[key])
                elif issubclass(value.type_, Base):
                    actual_dict[key] = self.marshal_to_class(value.type_, as_dict[key])
                elif value.type_ in [datetime.datetime, datetime.date]:
                    actual_dict[key] = value.type_.fromtimestamp(float(as_dict[key]))
                else:
                    actual_dict[key] = as_dict[key]
        result = klass(**actual_dict)
        return result

    def marshal_to_class(self, klass, redis_dictionary):
        if isinstance(redis_dictionary, klass):
            return redis_dictionary
        to_dict = json.loads(redis_dictionary)
        klass_dict = to_python_dict(to_dict, klass.model_fields())
        return klass(**klass_dict)


class CacheQuerySet(QuerySetMixin):
    def __init__(self, klass):
        self.klass = klass

    def cache_key(self, obj: typing.Union[typing.Any, str]):
        config = self.klass.Config
        if type(obj) == str:
            return f"{config.cache_key}:{obj}"
        if hasattr(config, "cache_field"):
            if type(obj) == dict:
                value = obj.get("email")
            else:
                value = getattr(obj, config.cache_field)
            return f"{config.cache_key}:{value}"
        raise AssertionError("Cache field not added to the Config class on the model.")

    async def save_to_cache(self, result, connection):
        _cache_key = self.cache_key(result)
        as_dict = self.dict_to_redis_dict(result)
        await connection.hmset_dict(_cache_key, as_dict)

    async def get(self, cache_key: str, connection=None):
        if connection:
            _cache_key = self.cache_key(cache_key)
            result = await connection.hgetall(_cache_key)
            from_db = False
            if not result:
                result = await self.klass.get_data(cache_key)
                await self.save_to_cache(result, connection)
                from_db = True
            result["from_db"] = from_db
            instance = self.redis_dict_to_obj(
                result, self.klass.model_fields(), self.klass
            )
            return instance


class QuerySet(QuerySetMixin):
    ESCAPE_CHARACTERS = ["%", "_"]

    def __init__(self, klass, pk="id"):
        self.klass = klass
        self.queryset = None
        self.pk = pk
        self._using = "default"

    @cached_property
    def metadata(self):
        return self.klass.metadata

    @property
    def database(self):
        return self.klass.databases.get(self._using)

    @cached_property
    def table(self):
        return self.klass.build_table(self.metadata)

    async def bulk_create_or_insert(self, records, is_model=True, force_create=False):
        assert len(records) > 0
        sql = self.table.insert()
        if is_model:
            values = [x.db_dict() for x in records]
        else:
            values = [self.klass.build_db_dict(x) for x in records]
        if not force_create:
            if values[0].get("id"):
                sql = self.table.update()
        for i in values:
            if i.get("id") in [0, None]:
                i.pop("id")
        # logging.info(values)
        async with self.database.transaction():
            await self.database.execute_many(query=sql, values=values)

    def get_queryset(self):
        if self.queryset == None:
            self.queryset = self.table.select()
        return self.queryset

    def build_select_expression(
        self, _model_cls, _filter_clauses, _select_related, limit_count=None
    ):
        tables = [self.table]
        select_from = self.table
        for item in _select_related:
            model_cls = _model_cls
            select_from = self.table
            for part in item.split("__"):
                model_cls = model_cls.get_related_field_class(part)
                select_from = sqlalchemy.sql.join(select_from, model_cls.table)
                tables.append(model_cls.table)

        expr = sqlalchemy.sql.select(tables)
        expr = expr.select_from(select_from)

        if _filter_clauses:
            if len(_filter_clauses) == 1:
                clause = _filter_clauses[0]
            else:
                clause = sqlalchemy.sql.and_(*_filter_clauses)
            expr = expr.where(clause)

        if limit_count:
            expr = expr.limit(limit_count)

        return expr

    def filter(self, **kwargs) -> sqlalchemy.sql.selectable.Select:
        filter_clauses = []
        select_related = []

        for key, value in kwargs.items():
            json_column = None
            json_filter = None
            if "__" in key:
                parts = key.split("__")

                # Determine if we should treat the final part as a
                # filter operator or as a related field.
                if parts[-1] in FILTER_OPERATORS:
                    op = parts[-1]
                    field_name = parts[-2]
                    related_parts = parts[:-2]
                    # check if field_name is a json_field
                    if not related_parts and self.klass.is_json_field(field_name):
                        json_column = field_name
                else:
                    op = "exact"
                    field_name = parts[-1]
                    related_parts = parts[:-1]

                model_cls = self.klass
                if related_parts:
                    # Add any implied select_related
                    related_str = "__".join(related_parts)
                    if related_str not in select_related:
                        if model_cls.is_related_field(related_str):
                            select_related.append(related_str)
                        else:
                            json_column = related_str

                    # Walk the relationships to the actual model class
                    # against which the comparison is being made.
                    for part in related_parts:
                        model_cls = model_cls.get_related_field_class(part)
                if json_column:
                    column = self.table.columns[json_column]
                    json_filter = field_name
                else:
                    column = model_cls.table.columns[field_name]

            else:
                op = "exact"
                column = self.table.columns[key]

            # Map the operation code onto SQLAlchemy's ColumnElement
            # https://docs.sqlalchemy.org/en/latest/core/sqlelement.html#sqlalchemy.sql.expression.ColumnElement
            op_attr = FILTER_OPERATORS[op]
            has_escaped_character = False
            if json_column:
                if op in JSON_OPERATORS.keys():
                    func = getattr(column.comparator, JSON_OPERATORS[op])
                    if type(value) == dict:
                        clause = func(value)
                    else:
                        has_escaped_character = any(
                            c for c in self.ESCAPE_CHARACTERS if c in value
                        )
                        if has_escaped_character:
                            # enable escape modifier
                            for char in self.ESCAPE_CHARACTERS:
                                value = value.replace(char, f"\\{char}")
                        value = f"%{value}%"
                        clause = column.comparator[json_filter].astext.ilike(value)
                else:
                    # clause = (
                    #     sqlalchemy.cast(
                    #         column.comparator[json_filter], sqlalchemy.String
                    #     )
                    #     == value
                    # )
                    clause = (
                        column.comparator[json_filter].astext == value
                    )  # only equality for now
            else:
                if op in ["contains", "icontains"]:
                    has_escaped_character = any(
                        c for c in self.ESCAPE_CHARACTERS if c in value
                    )
                    if has_escaped_character:
                        # enable escape modifier
                        for char in self.ESCAPE_CHARACTERS:
                            value = value.replace(char, f"\\{char}")
                    value = f"%{value}%"
                if isinstance(value, self.klass.__class__):
                    value = "id"

                # import ipdb

                # ipdb.set_trace()
                clause = getattr(column, op_attr)(value)
                clause.modifiers["escape"] = "\\" if has_escaped_character else None
            filter_clauses.append(clause)
        # params = [getattr(self.table.c, key) == value for key, value in kwargs.items()]
        # self.queryset = self.get_queryset().where(sqlalchemy.and_(*params))
        self.queryset = self.build_select_expression(
            self.klass, filter_clauses, select_related
        )
        return self

    async def get(self, **kwargs):
        params = [getattr(self.table.c, key) == value for key, value in kwargs.items()]
        sql = self.get_queryset().where(sqlalchemy.and_(*params))
        result = await self.database.fetch_one(sql)
        self.queryset = None
        if result:
            return await self.as_klass(result)

    async def as_dict(self, sqlalchemy_result):
        result = {}
        for key, value in sqlalchemy_result.items():
            klass_field = self.klass.get_class_field_from_db_name(key)
            if klass_field:
                if self.klass.is_related_field(klass_field[0]):
                    cls_instance = utils.get_field(klass_field[1])
                    instance = await cls_instance.objects.get(id=value)
                    result[klass_field[0]] = instance
                else:
                    result[klass_field[0]] = value
        return result

    async def as_klass(self, sqlalchemy_result):
        _dict = await self.as_dict(sqlalchemy_result)
        return self.klass(**_dict)

    async def all(self, where_clause=None, custom=False):
        queryset = where_clause if custom else self.get_queryset()
        result = await self.database.fetch_all(queryset)
        self.queryset = None
        return await asyncio.gather(*[self.as_klass(o) for o in result])

    async def count(self):
        query = (
            sqlalchemy.func.count()
            .select()
            .select_from(self.get_queryset().alias("ss"))
        )
        self.queryset = None  # clear for new calls
        return await self.database.fetch_val(query)

    async def exists(self):
        # query = (
        #     .select()
        #     .select_from(self.get_queryset().alias("ss"))
        # )
        count = await self.count()
        return count > 0

    async def update_kwargs_for_creation(self, **kwargs):
        new_kwargs = await self.klass.transform_kwargs(**kwargs)
        missing_kwargs = self.klass.update_passed_values(new_kwargs)
        new_kwargs = {**new_kwargs, **missing_kwargs}
        return new_kwargs

    async def create(self, **kwargs):
        # a check to see if any foreign key exists in the kwargs
        new_kwargs = await self.update_kwargs_for_creation(**kwargs)
        # new_kwargs = await self.klass.transform_kwargs(**kwargs)
        # missing_kwargs = self.klass.update_passed_values(new_kwargs)
        # new_kwargs = {**new_kwargs, **missing_kwargs}
        instance = self.klass(**new_kwargs)
        await instance.save(using=self._using)
        return instance

    async def update(self, **kwargs):
        queryset = self.get_queryset()
        where_clause = queryset._whereclause
        queryset = self.table.update().values(**kwargs).where(where_clause)
        result = await self.database.execute(queryset)
        return result

    async def first(self):
        expr = self.get_queryset()
        key = getattr(self.table.c, self.pk)
        expr = expr.order_by(key.asc())
        result = await self.database.fetch_one(expr)
        self.queryset = None
        if result:
            return await self.as_klass(result)
        return None

    async def last(self):
        expr = self.get_queryset()
        key = getattr(self.table.c, self.pk)
        expr = expr.order_by(key.desc())
        result = await self.database.fetch_one(expr)
        self.queryset = None
        if result:
            return await self.as_klass(result)
        return None

    async def delete(self):
        expr = self.get_queryset().alias()
        where_clause = self.table.c.id.in_(
            sqlalchemy.select([self.table.c.id]).where(self.table.c.id == expr.c.id)
        )
        sql = sqlalchemy.sql.delete(self.table).where(where_clause)
        self._using = "default"
        result = await self.database.execute(sql)
        self.queryset = None
        return result

    def order_by(self, *args):
        self.queryset = self.get_queryset().order_by(*args)
        return self

    def as_sql(self):
        q = self.get_queryset()
        return {"query": str(q), "values": q.compile().params}

    def using(self, key):
        self._using = key
        return self

    def cache_key(self, obj: typing.Union[typing.Any, str]):
        config = self.klass.Config
        if type(obj) == str:
            return f"{config.table_name}:{obj}"
        if hasattr(config, "cache_field"):
            value = getattr(obj, config.cache_field)
            return f"{config.table_name}:{value}"
        raise AssertionError("Cache field not added to the Config class on the model.")

    # CACHE Queryset methods
    async def quick_create(self, connection, **kwargs):
        new_kwargs = await self.update_kwargs_for_creation(**kwargs)
        instance = self.klass(**new_kwargs)
        as_dict = self.obj_to_redis_dict(instance)
        # remove demo_id
        as_dict.pop("id", None)
        key = self.cache_key(instance)
        in_cache = await connection.exists(key)
        if in_cache:
            raise exceptions.CacheDuplicateError(f"Item with {key} previously saved")
        await connection.hmset_dict(key, as_dict)
        return key

    async def cache_get(self, cache_field: str, connection=None, auto_close=False):
        key = self.cache_key(cache_field)

        if connection:
            record = await connection.hgetall(key)
            if auto_close:
                connection.close()
                await connection.wait_closed()
            if record:
                obj = self.redis_dict_to_obj(record)
                return obj

    async def in_write_cache(self, obj, connection=None) -> bool:
        key = self.cache_key(obj)
        result = await connection.exists(key)
        return bool(result)

    async def remove_cache(self, obj, connection=None):
        if connection:
            exists = await self.in_write_cache(obj, connection=connection)
            key = self.cache_key(obj)
            if exists:
                await connection.delete(key)

    async def cache_all(self, connection=None):
        if connection:
            prefix = self.klass.Config.table_name
            keys = await connection.keys(f"*{prefix}:*")
            if keys:
                pipe = connection.pipeline()
                tasks = [pipe.hgetall(x) for x in keys]
                result = await pipe.execute()
                data = await asyncio.gather(*tasks)
                return [self.redis_dict_to_obj(x) for x in data]
            return []
        raise AssertionError("aioredis connection not passed")

    def obj_to_redis_dict(self, obj):
        return super().obj_to_redis_dict(obj, self.klass.model_fields())

    def redis_dict_to_obj(self, as_dict: dict):
        return super().redis_dict_to_obj(as_dict, self.klass.model_fields(), self.klass)
