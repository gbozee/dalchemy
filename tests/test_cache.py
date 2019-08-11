import models
from orm.utils import async_adapter
import pytest
from orm.exceptions import CacheDuplicateError
import asyncio

# two different storage location cache
# a read cache location and a write cache location.


@pytest.mark.run_loop
async def test_record_create(create_redis):
    conn = await create_redis()
    await conn.delete("users:j@o.com")
    cache_key = await models.User.objects.quick_create(
        conn, full_name="Abiola", email="j@o.com"
    )
    assert cache_key == "users:j@o.com"
    record = await models.User.objects.cache_get("j@o.com", connection=conn)
    assert record.full_name == "Abiola"
    assert record.email == "j@o.com"
    assert not record.id
    assert await record.in_write_cache(conn)
    async with record.database:
        instance = await record.save(connection=conn)
        # removed from the write cache
        assert not await record.in_write_cache(conn)
        # place in read cache
        # assert await record.in_read_cache()
        assert record.id
        # trying to get item from cache returns record from the db instead since
        # item no longer in cache.
        record = await models.User.objects.cache_get("j@o.com")
        assert not record
        await models.User.objects.delete()


@pytest.mark.run_loop
async def test_prevent_duplicate_record_in_cache(create_redis):
    conn = await create_redis()
    await conn.delete("users:j@o.com")
    cache_key = await models.User.objects.quick_create(
        conn, full_name="Abiola", email="j@o.com"
    )
    with pytest.raises(CacheDuplicateError):
        await models.User.objects.quick_create(conn, full_name="Shola", email="j@o.com")


@pytest.mark.run_loop
async def test_queryset_for_specific_model_from_cache(create_redis):
    conn = await create_redis()
    await conn.delete("users:j@o.com")
    await conn.delete("skills:French")
    await models.User.objects.quick_create(conn, full_name="Abiola", email="j@o.com")
    await models.Skill.objects.quick_create(conn, name="French")

    records = await models.User.objects.cache_all(connection=conn)
    assert len(records) == 1
    assert records[0].full_name == "Abiola"
    assert records[0].email == "j@o.com"
    assert not records[0].id


async def create_test_data():
    user = await models.User.objects.create(
        full_name="Jones@example.com", email="j@o.com"
    )
    await asyncio.gather(
        models.Profile.objects.create(user=user, addresses=[{"state": "Lagos"}]),
        models.PhoneNumber.objects.create(user=user, number="+2347833224323"),
    )


@pytest.mark.run_loop
async def test_cache_model(create_redis):
    conn = await create_redis()
    await conn.delete("user_info:j@o.com")
    await conn.delete("email:j@o.com")
    db = models.User.database
    async with db:  # using default database
        await create_test_data()
        record = await models.UserInfo.objects.get("j@o.com", connection=conn)
        assert record.full_name == "Jones@example.com"
        assert record.email == "j@o.com"
        assert record.is_active
        assert record.addresses == [{"state": "Lagos"}]
        assert record.numbers == ["+2347833224323"]
        assert record.cache_id == "user_info:j@o.com"
        assert record.from_db
        record = await models.UserInfo.objects.get("j@o.com", connection=conn)
        assert not record.from_db
        await models.User.objects.delete()
        await models.Profile.objects.delete()
        await models.PhoneNumber.objects.delete()
        # update cache
        record.full_name = "J2o@example.com"
        await record.update_cache(conn)
        record = await models.UserInfo.objects.get("j@o.com", connection=conn)
        assert record.full_name == "J2o@example.com"

