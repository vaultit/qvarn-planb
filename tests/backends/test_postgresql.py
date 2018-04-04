from qvarn.backends.postgresql import chop_long_name
from qvarn.backends.postgresql import get_new_id
from qvarn.backends.postgresql import flatten_for_lists
from qvarn.backends.postgresql import flatten_for_gin


def test_get_new_id():
    random_field = '448134794a2f6da110a178def79d1d8f'
    assert get_new_id('test', random_field) == 'ee26-448134794a2f6da110a178def79d1d8f-e954e909'


def test_chop_long_name():
    name = 'foo_bar_baz_' * 10
    assert chop_long_name(name, 18) == 'foo_bar_baz_a1325b'


def test_flatten_for_lists():
    data = {
        'a': 1,
        'b': [2, 3],
        'c': [
            {'d': 4},
        ],
        'd': 5,
        'e': {
            'f': 6,
        },
    }
    assert flatten_for_lists(data) == [
        {
            'a': 1,
            'b': 2,
            'd': 5,
            'f': 6,
        },
        {
            'b': 3,
            'd': 4,
        },
    ]


def test_flatten_for_gin():
    data = {
        'a': 1,
        'b': [2, 3],
        'c': [
            {'d': 4},
        ],
        'd': 5,
        'e': {
            'f': 6,
        },
    }
    sort_key = lambda x: list(x.items())[0]  # noqa
    assert sorted(flatten_for_gin(data), key=sort_key) == [
        {'a': 1},
        {'b': 2},
        {'b': 3},
        {'d': 4},
        {'d': 5},
        {'f': 6},
    ]
