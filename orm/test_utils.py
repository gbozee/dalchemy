import asyncio

import aioredis
import databases
import pytest
import sqlalchemy
from async_timeout import timeout as async_timeout


@pytest.fixture
def closable(loop):
    conns = []

    try:
        yield conns.append
    finally:
        waiters = []
        while conns:
            conn = conns.pop(0)
            if not conn.closed:
                conn.close()
                waiters.append(conn.wait_closed())
        if waiters:
            loop.run_until_complete(asyncio.gather(*waiters, loop=loop))


@pytest.yield_fixture
def loop():
    """Creates new event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(None)

    try:
        yield loop
    finally:
        if hasattr(loop, "is_closed"):
            closed = loop.is_closed()
        else:
            closed = loop._closed  # XXX
        if not closed:
            loop.call_soon(loop.stop)
            loop.run_forever()
            loop.close()


async def _wait_coro(corofunc, kwargs, timeout, loop):
    with async_timeout(timeout, loop=loop):
        return await corofunc(**kwargs)


@pytest.mark.tryfirst
def pytest_pyfunc_call(pyfuncitem):
    """
    Run asyncio marked test functions in an event loop instead of a normal
    function call.
    """
    marker = pyfuncitem.get_closest_marker("run_loop")
    if marker is not None:
        funcargs = pyfuncitem.funcargs
        loop = funcargs["loop"]
        testargs = {arg: funcargs[arg] for arg in pyfuncitem._fixtureinfo.argnames}

        loop.run_until_complete(
            _wait_coro(
                pyfuncitem.obj,
                testargs,
                timeout=marker.kwargs.get("timeout", 15),
                loop=loop,
            )
        )
        return True


def pytest_runtest_setup(item):
    run_loop = item.get_closest_marker("run_loop")
    if run_loop and "loop" not in item.fixturenames:
        # inject an event loop fixture for all async tests
        item.fixturenames.append("loop")


class MockRequest(object):
    def __init__(self, response, **kwargs):
        self.response = response
        self.overwrite = True
        if kwargs.get("overwrite"):
            self.overwrite = True
        self.status_code = kwargs.get("status_code", 200)

    @classmethod
    def raise_for_status(cls):
        pass

    @property
    def text(self):
        return json.dumps(self.response)

    def json(self):
        if self.overwrite:
            return self.response
        return {"data": self.response}


@pytest.fixture
def mock_response():
    def _mock_response(*args, **kwargs):
        return MockRequest(*args, **kwargs)

    return _mock_response
