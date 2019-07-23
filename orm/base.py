import typing

import databases
import pydantic
import sqlalchemy
from pydantic import BaseModel, EmailStr, SecretStr
from pydantic.dataclasses import dataclass


@dataclass
class Field:
    name: str
    type: typing.Any


def create_db_column(field: Field, **kwargs) -> sqlalchemy.Column:
    field_name = field.name
    field_type = field.type_
    if field_type == int:
        column = sqlalchemy.Integer
    elif field_type in [str, EmailStr, SecretStr]:
        column = sqlalchemy.String
    elif field_type == bool:
        column = sqlalchemy.Boolean
    return sqlalchemy.Column(field_name, column, **kwargs)


class Base(BaseModel):
    @classmethod
    def build_table(cls, metadata: sqlalchemy.MetaData) -> sqlalchemy.Table:
        columns = []
        table_name = cls.Config.table_name
        config = cls.Config.table_config or {}
        for key, value in cls.__dict__["__fields__"].items():
            if key not in ["table_config", "table_name"]:
                attr = config.get(key) or {}
                columns.append(create_db_column(value, **attr))
        return sqlalchemy.Table(
            table_name, metadata, *columns, extend_existing=True
        )

     
    def get_table(cls, metadata:sqlalchemy.MetaData):
        return self.__class__.build_table(metadata)


    async def save(self, database:databases.Database,metadata:sqlalchemy.MetaData):
        table = self.get_table(metadata)
        values = self.dict()
        if not self.id:
            sql = table.insert()
            values.pop('id')
        else:
            sql = table.update()
        sql = sql.values(**values)
        await database.execute(sql)

    async def bulk_create_or_insert(self, records:typing.List[pydantic.BaseModel], database:databases.Database, metadata:sqlalchemy.MetaData):
        table = self.get_table(metadata)
        sql = table.insert()
        values = [x.dict() for x in records]
        if values[0]['id'] != 0:
            sql = table.update()
        for i in values:
            if i == 0:
                i.pop('id')
        await database.execute_many(query=sql, values=values)
