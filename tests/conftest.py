import asyncio

import pytest
import apistar

from qvarn.app import get_app


@pytest.fixture(scope='session')
def app():
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(get_app())


@pytest.fixture()
def client(app):
    return apistar.TestClient(app)
