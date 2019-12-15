import asyncio
import functools
import sqlalchemy


def init_tables(model: type, database, replica_database=None, redis_instance=None):
    tables = []
    metadata = sqlalchemy.MetaData()
    for subclass in model.__subclasses__():
        t = subclass.build_table(metadata)
        tables.append(t)
    for subclass in model.__subclasses__():
        subclass.init_db_params(
            {"default": database, "replica": replica_database},
            metadata,
            redis_instance=redis_instance
            # database, metadata, other_db={"replica": replica_database}
        )
    return metadata


def async_adapter(wrapped_func):
    """
    Decorator used to run async test cases.
    """

    @functools.wraps(wrapped_func)
    def run_sync(*args, **kwargs):
        loop = asyncio.get_event_loop()
        task = wrapped_func(*args, **kwargs)
        return loop.run_until_complete(task)

    return run_sync


def get_field(field):
    classes = field.__args__
    value = [x for x in field.__args__ if x.__class__.__name__ == "ModelMetaClass"]
    return value[0]
