import databases
import pytest
import sqlalchemy

import models
from orm.utils import async_adapter


async def clean_db():
    await models.User.objects.delete()
    await models.Profile.objects.delete()
    await models.PhoneNumber.objects.delete()
    await models.Skill.objects.delete()


@pytest.mark.run_loop
async def test_model_create():
    user = models.User(full_name="Abiola", email="j@o.com")
    async with user.database:
        await clean_db()
        assert not user.id
        await user.save()
        assert user.id == 1
        table = models.User.table
        sql = table.select()
        result = await user.database.fetch_all(sql)
        assert len(result) == 1
        assert await models.User.objects.count() == 1
        user.full_name = "John Doe"
        await user.save()
        result = await models.User.database.fetch_all(
            sql.where(models.User.c.full_name == "John Doe")
        )
        assert len(result) == 1
        result = await user.database.fetch_all(sql.where(table.c.full_name == "Abiola"))
        assert await models.User.objects.filter(full_name="Abiola").count() == 0
        result = await models.User.objects.get(full_name="John Doe")
        assert result.full_name == "John Doe"
        assert result.is_active
        result = await models.User.objects.filter(full_name="John Doe").update(
            is_active=False
        )
        result = await models.User.objects.filter(is_active=False).get()
        assert result.full_name == "John Doe"
        assert not result.is_active
        record = await models.User.objects.create(full_name="John", email="j2@o.com")
        assert record.full_name == "John"
        assert await models.User.objects.count() == 2
        assert await models.User.objects.filter(full_name="John").exists()
        await models.User.objects.delete()
        assert await models.User.objects.count() == 0


@pytest.mark.run_loop
async def test_date_create_or_update():
    async with models.User.database:
        await clean_db()
        user = await models.User.objects.create(full_name="Abiola", email="j@o.com")
        print(user.created)
        print(user.modified)
        await user.save()
        print(user.created)
        print(user.modified)


@pytest.mark.run_loop
async def test_foreign_key_relationship_creation_with_id():
    user = models.User(full_name="Abiola", email="j@o.com")
    async with user.database:
        await clean_db()
        await user.save()
        user_number = await models.PhoneNumber.objects.create(
            user_id=user.id, number="+2348033223323"
        )
        assert user_number.user.id == user.id
        assert user_number.number == "+2348033223323"
        assert user_number.user.full_name == "Abiola"
        # fetch from db
        record = await models.PhoneNumber.objects.first()
        assert record.user.id == user.id
        assert record.number == "+2348033223323"
        assert record.user.full_name == "Abiola"
        # fetch with related data
        record = await models.PhoneNumber.objects.last()
        assert record.user.full_name == "Abiola"
        # test with filter
        table = models.PhoneNumber.table
        sql = table.select()
        record = await models.PhoneNumber.objects.filter(
            **{"user__full_name": "Abiola"}
        ).get()
        assert record.user.full_name == "Abiola"
        assert record.number == "+2348033223323"
        await models.User.objects.delete()
        await models.PhoneNumber.objects.delete()


@pytest.mark.run_loop
async def test_foreign_key_relationship():
    user = models.User(full_name="Abiola", email="j@o.com")
    async with user.database:
        await clean_db()
        await user.save()
        user_number = await models.PhoneNumber.objects.create(
            user=user, number="+2348033223323"
        )
        assert user_number.user.id == user.id
        assert user_number.number == "+2348033223323"
        assert user_number.user.full_name == "Abiola"
        # fetch from db
        record = await models.PhoneNumber.objects.first()
        assert record.user.id == user.id
        assert record.number == "+2348033223323"
        assert record.user.full_name == "Abiola"
        # fetch with related data
        record = await models.PhoneNumber.objects.last()
        assert record.user.full_name == "Abiola"
        # test with filter
        table = models.PhoneNumber.table
        sql = table.select()
        record = await models.PhoneNumber.objects.filter(
            **{"user__full_name": "Abiola"}
        ).get()
        assert record.user.full_name == "Abiola"
        assert record.number == "+2348033223323"
        await models.User.objects.delete()
        await models.PhoneNumber.objects.delete()


@pytest.mark.run_loop
async def test_bulk_json_field_insert():
    user = models.User(full_name="Abiola", email="j@o.com")
    async with user.database:
        await clean_db()
        await user.save()
        await models.Profile.objects.bulk_create_or_insert(
            [
                dict(user_id=user.id, addresses={"name": "Eleja 2"}),
                dict(user_id=user.id, addresses={"name": "Sunday 2"}),
                dict(user_id=user.id, addresses={"name": "Sunday"}),
            ],
            is_model=False,
        )
        profile = await models.Profile.objects.first()
        assert profile.addresses == {"name": "Eleja 2"}
        assert profile.user == user


@pytest.mark.run_loop
async def test_bulk_json_field_insert_with_custom_id():
    user = models.User(full_name="Abiola", email="j@o.com")
    async with user.database:
        await clean_db()
        await user.save()
        await models.Profile.objects.bulk_create_or_insert(
            [
                dict(id=3, user_id=user.id, addresses={"name": "Eleja 2"}),
                dict(id=2, user_id=user.id, addresses={"name": "Sunday 2"}),
                dict(id=1, user_id=user.id, addresses={"name": "Sunday"}),
            ],
            is_model=False,
            force_create=True,
        )
        profile = await models.Profile.objects.get(id=3)
        assert profile.addresses == {"name": "Eleja 2"}
        assert profile.user == user
        assert profile.id == 3


@pytest.mark.run_loop
async def test_json_field_query():
    user = models.User(full_name="Abiola", email="j@o.com")
    async with user.database:
        await clean_db()
        await user.save()
        await models.Profile.objects.bulk_create_or_insert(
            [
                models.Profile(user=user, addresses={"name": "Eleja 2"}),
                models.Profile(user=user, addresses={"name": "Sunday 2"}),
                models.Profile(user=user, addresses={"name": "Sunday"}),
            ]
        )
        profile = await models.Profile.objects.first()
        assert profile.addresses == {"name": "Eleja 2"}
        assert profile.user == user
        last = await models.Profile.objects.last()
        assert last.addresses == {"name": "Sunday"}
        # simple filter for json objects
        record = await models.Profile.objects.filter(addresses__name="Sunday").get()
        assert record.addresses == {"name": "Sunday"}
        record = await models.Profile.objects.filter(
            addresses__name__contains="2"
        ).count()
        assert record == 2
        record = await models.Profile.objects.filter(
            addresses__name__icontains="sun"
        ).count()
        assert record == 2
        record = await models.Profile.objects.filter(
            addresses__contains={"name": "Sunday"}
        ).count()
        assert record == 1
        current_count = await models.Profile.objects.count()
        assert current_count == 3
        await models.Profile.objects.filter(
            addresses__contains={"name": "Sunday"}
        ).delete()
        current_count = await models.Profile.objects.count()
        assert current_count == 2
        await models.User.objects.delete()
        await models.Profile.objects.delete()


@pytest.mark.run_loop
async def test_different_db_usage(replica_database):
    db = models.User.database
    async with db:  # using default database
        await clean_db()
        await models.User.objects.delete()
        await models.Profile.objects.delete()
        user = await models.User.objects.create(full_name="Abiola", email="j@o.com")
        assert await models.User.objects.count() == 1

    async with replica_database:
        assert await models.User.objects.using("replica").count() == 1

    await db.connect()
    await models.User.objects.delete()
    await db.disconnect()


# @async_adapter
# async def test_queryset_create():
