import asyncio
import pathlib

import pytest
import apistar

from qvarn import backends
from qvarn.app import get_app


SETTINGS = {
    'QVARN': {
        'BACKEND': {
            'MODULE': 'qvarn.backends.postgresql',
            'USERNAME': None,
            'PASSWORD': None,
            'HOST': None,
            'PORT': None,
            'DBNAME': 'planbtest',
            'INITDB': False,
        },
        'RESOURCE_TYPES_PATH': str(pathlib.Path(__file__).parent / 'resources'),
    },
}


@pytest.fixture(scope='session')
def storage():
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(backends.init(SETTINGS))


@pytest.fixture(scope='session')
def app():
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(get_app(settings=SETTINGS))


@pytest.fixture()
def client(app):
    return apistar.TestClient(app)
