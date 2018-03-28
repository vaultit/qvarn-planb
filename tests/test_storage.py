import configparser
import pathlib

import aiopg.sa
import pytest
import ruamel.yaml as yaml
import sqlalchemy as sa

from qvarn.storage import Storage
from qvarn.storage import chop_long_name
from qvarn.storage import get_new_id
from qvarn.storage import iter_lists


def test_get_new_id():
    random_field = '448134794a2f6da110a178def79d1d8f'
    assert get_new_id('test', random_field) == 'ee26-448134794a2f6da110a178def79d1d8f-e954e909'


def test_chop_long_name():
    name = 'foo_bar_baz_' * 10
    assert chop_long_name(name, 18) == 'foo_bar_baz_a1325b'


def test_separate_lists():
    data = {
        "a": {
            "b": 1,
            "c": [42],
        },
        "d": [
            {"x": 1},
            {"x": 2},
        ],
        "e": [
            [1, 2],
            [3, 4],
        ],
    }
    assert list(iter_lists(data)) == [
        (('a', 'c'), [42]),
        (('d',), [{'x': 1}, {'x': 2}]),
        (('e',), [1, 2, 3, 4]),
    ]


def gin_query(proto, key, value, path=()):
    if isinstance(proto, dict):
        # TODO:
        pass


def _test_gin_query():
    prototype = {'a': [{'b': 0}], 'c': 0}
    assert gin_query(prototype, 'b', 42) == {'a': [{'b': 42}]}


@pytest.mark.asyncio
async def test_create():
    engine = sa.create_engine('postgresql:///planb', echo=False)

    pool = await aiopg.sa.create_engine(database='planb')

    storage = Storage(engine, pool)
    config = configparser.RawConfigParser()
    config.read('qvarn.cfg')
    for path in sorted(pathlib.Path(config.get('qvarn', 'resource-types')).glob('*.yaml')):
        schema = yaml.safe_load(path.read_text())
        storage.add_resource_type(schema)

    data = {
        'type': 'contract',
        'contract_type': 'tilaajavastuu_account',
        'preferred_language': 'lt',
        'contract_parties': [
            {
                'type': 'person',
                'role': 'user',
                'resource_id': 'person-id',
            },
        ],
    }

    row_id = await storage.create('contracts', data)
    row = await storage.get('contracts', row_id)
    assert row == dict(data, id=row_id, revision=row['revision'])
    assert row_id in await storage.list('contracts')
