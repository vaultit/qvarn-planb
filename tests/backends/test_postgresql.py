import configparser
import pathlib

import aiopg.sa
import pytest
import ruamel.yaml as yaml
import sqlalchemy as sa

from qvarn.backends.postgresql import PostgreSQLStorage
from qvarn.backends.postgresql import chop_long_name
from qvarn.backends.postgresql import get_new_id
from qvarn.backends.postgresql import iter_lists
from qvarn.backends.postgresql import FlatField
from qvarn.backends.postgresql import flatten
from qvarn.backends.postgresql import update_gin_query


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


def test_flatten():
    data = {
        'a': [
            {'b': 3},
            {'b': [1, 2], 'a': '4'},
        ],
        'c': 42,
    }
    data_subpath = {
        'x': 0,
    }
    assert flatten((data, data_subpath)) == {
        'a': {
            FlatField(('a', '', 'a'), '4', True),
        },
        'b': {
            FlatField(('a', '', 'b'), 3, True),
            FlatField(('a', '', 'b', ''), 1, True),
            FlatField(('a', '', 'b', ''), 2, True),
        },
        'c': {
            FlatField(('c',), 42, False),
        },
        'x': {
            FlatField(('x',), 0, False),
        }
    }


def test_update_git_query():
    assert update_gin_query(None, ('a',), 1) == {'a': 1}
    assert update_gin_query(None, ('',), 1) == [1]
    assert update_gin_query(None, ('a', '', 'b'), 1) == {'a': [{'b': 1}]}
    assert update_gin_query({'a': 1}, ('b',), 2) == {'a': 1, 'b': 2}
    assert update_gin_query([1, 2], ('',), 3) == [1, 2, 3]


@pytest.mark.asyncio
async def test_create():
    engine = sa.create_engine('postgresql:///planb', echo=False)

    pool = await aiopg.sa.create_engine(database='planb')

    storage = PostgreSQLStorage(engine, pool)
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

    row = await storage.create('contracts', data)
    row = await storage.get('contracts', row['id'])
    assert row == dict(data, id=row['id'], revision=row['revision'])
    assert row['id'] in await storage.list('contracts')
