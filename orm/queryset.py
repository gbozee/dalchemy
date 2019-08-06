import typing
from dataclasses import dataclass

import databases
import sqlalchemy
from cached_property import cached_property
from .fields import CustomField
from . import utils


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


class QuerySet:
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

    async def bulk_create_or_insert(self, records):
        sql = self.table.insert()
        values = [x.db_dict() for x in records]
        if values[0]["id"] != 0:
            sql = self.table.update()
        for i in values:
            if i["id"] == 0:
                i.pop("id")
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

    async def all(self):
        result = await self.database.fetch_all(self.get_queryset())
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

    async def create(self, **kwargs):
        instance = self.klass(**kwargs)
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
        return await self.as_klass(result)

    async def last(self):
        expr = self.get_queryset()
        key = getattr(self.table.c, self.pk)
        expr = expr.order_by(key.desc())
        result = await self.database.fetch_one(expr)
        self.queryset = None
        return await self.as_klass(result)

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
