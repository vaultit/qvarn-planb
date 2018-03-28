def test_version(client):
    data = client.get('/version').json()
    data['api']['version'] = '0.82'
    data['implementation']['version'] = '0.0.1'
    assert data == {
        'api': {
            'version': '0.82',
        },
        'implementation': {
            'name': 'Qvarn PlanB',
            'version': '0.0.1',
        },
    }


def test_create_get_list(client):
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

    # create
    row = client.post('/contracts', json=data).json()
    assert row == dict(data, id=row['id'], revision=row['revision'])

    # get
    row = client.get('/contracts/' + row['id']).json()
    assert row == dict(data, id=row['id'], revision=row['revision'])

    # list
    rows = client.get('/contracts').json()
    ids = {x['id'] for x in rows['resources']}
    assert row['id'] in ids
