from qvarn.utils import merge


def test_merge():
    assert merge({'a': 1}, {'a': 2}) == {'a': 2}
    assert merge({'a': 1}, {'b': 1}) == {'a': 1, 'b': 1}
    assert merge([1], [2]) == [1, 2]
    assert merge({'a': [1]}, {'a': [2]}) == {'a': [1, 2]}
    assert merge({'a': {'b': 1, 'c': 3}}, {'a': {'c': 2, 'd': 3}}) == {'a': {'b': 1, 'c': 2, 'd': 3}}
