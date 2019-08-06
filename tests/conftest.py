import databases
import pytest
import sqlalchemy

from models import Base
from orm import utils

# DATABASE_URL = "sqlite:///test.db"
DATABASE_URL = "postgresql://e_bots:e_bots1991@localhost:5434/e_bots"
REPLICA_DATABASE_URL = "postgresql://e_bots1:password@localhost:5434/e_bots"


@pytest.fixture(scope="module")
def metadata(database, replica_database):
    metadata = utils.init_tables(Base, database, replica_database)
    return metadata


@pytest.fixture(scope="module")
def database():
    return databases.Database(DATABASE_URL)


@pytest.fixture(scope="module")
def replica_database():
    return databases.Database(REPLICA_DATABASE_URL)


@pytest.fixture(autouse=True, scope="module")
def create_test_database(metadata):
    engine = sqlalchemy.create_engine(DATABASE_URL)
    metadata.create_all(engine)
    yield
    metadata.drop_all(engine)
