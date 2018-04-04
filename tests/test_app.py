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


def test_create_get_list_put(client):
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
    data['id'] = row['id']
    data['revision'] = row['revision']
    assert row == data

    # get
    row = client.get('/contracts/' + data['id']).json()
    assert row == data

    # list
    rows = client.get('/contracts').json()
    ids = {x['id'] for x in rows['resources']}
    assert data['id'] in ids

    # put
    data['preferred_language'] = 'fi'
    data['contract_parties'][0]['resource_id'] = 'changed'
    row = client.put('/contracts/' + row['id'], json=data).json()
    assert row['revision'] != data['revision']
    assert row == dict(data, revision=row['revision'])

    # get after put
    row = client.get('/contracts/' + row['id']).json()
    assert row['revision'] != data['revision']
    assert row == dict(data, revision=row['revision'])


def test_duplicate_names_case(client):
    data = {
        'type': 'org',
        'country': 'fi',
        'names': ['Orgtra'],
        'gov_org_ids': [
            {
                'country': 'lt',
                'org_id_type': 'testid',
                'gov_org_id': 'ORG-0001',
            },
        ],
        'contacts': [
            {
                'contact_type': 'home',
                'contact_roles': ['role'],
                'country': 'ua',
            }
        ]
    }

    row = client.post('/orgs', json=data).json()
    assert row == dict(data, id=row['id'], revision=row['revision'])

    row = client.get('/orgs/' + row['id']).json()
    assert row == dict(data, id=row['id'], revision=row['revision'])


def test_wrong_revision(client):
    data = {
        'type': 'contract',
        'contract_type': 'original',
    }

    # Create a resource.
    row = client.post('/contracts', json=data).json()
    data['id'] = row['id']
    data['revision'] = row['revision']

    # Try to update the resource with incorrect revision.
    resp = client.put('/contracts/' + data['id'], json=dict(data, contract_type='changed', revision='wrong'))
    assert resp.status_code == 409
    assert resp.json() == {
        'error_code': 'WrongRevision',
        'item_id': data['id'],
        'current': data['revision'],
        'update': 'wrong',
        'message': 'Updating resource {item_id} failed: resource currently has '
                   'revision {current}, update wants to update {update}.',
    }

    # Data should not be updated.
    row = client.get('/contracts/' + data['id']).json()
    assert row == data


def test_subresource(client):
    person = {
        "names": [
            {
                "full_name": "James Bond",
                "sort_key": "Bond, James",
                "titles": ["Päällikkö", "Seppä"],
                "given_names": ["James", "詹姆斯"],
                "surnames": ["Bond"],
            }
        ],
    }

    # Create a person resource.
    row = client.post('/persons', json=person).json()
    person['id'] = row['id']
    person['revision'] = row['revision']

    private = {
        'revision': person['revision'],
        'date_of_birth': '1920-11-11',
        'gov_ids': [
            {
                'country': 'GB',
                'id_type': 'ssn',
                'gov_id': 'SN 00 70 07'
            }
        ],
        'nationalities': ['GB'],
        'residences': [
            {
                'country': 'GB',
                'location': 'London'
            },
            {
                'country': 'FI',
                'location': 'Ypäjä'
            },
            {
                'country': 'NO',
                'location': 'un/known'
            },
            {
                'country': 'SE',
                'location': 'search'
            }
        ],
        'contacts': [
            {
                'contact_type': 'phone',
                'contact_source': 'self',
                'contact_timestamp': '2038-02-28T01:02:03+0400',
                'phone_number': '+358 4321'
            },
            {
                'contact_type': 'email',
                'contact_source': 'self',
                'contact_timestamp': '2038-02-28T01:02:03+0400',
                'email_address': 'james.bond@sis.gov.uk'
            },
            {
                'contact_type': 'address',
                'contact_source': 'self',
                'contact_timestamp': '2038-02-28T01:02:03+0400',
                'country': 'GB',
                'full_address': '61 Horsen Ferry Road\\nLondon S1',
                'address_lines': ['61 Horsen Ferry Road'],
                'post_code': 'S1',
                'post_area': 'London'
            }
        ]
    }

    # Update subresource of just created person resource.
    row = client.put('/persons/%s/private' % person['id'], json=private).json()
    private['revision'] = row['revision']
    assert private['revision'] != person['revision']
    assert row == private

    # Get subresource.
    row = client.get('/persons/%s/private' % person['id']).json()
    assert row == private

    # Update subresource.
    private['date_of_birth'] = '1920-12-12'
    row = client.put('/persons/%s/private' % person['id'], json=private).json()
    assert row['revision'] != private['revision']

    # Get updated subresource.
    row = client.get('/persons/%s/private' % person['id']).json()
    private['revision'] = row['revision']
    assert row == private


def test_search_exact(client, storage):
    storage.wipe_all_data('orgs')

    a = client.post('/orgs', json={
        'names': ['Company 1', 'The Company'],
        'country': 'FI',
        'gov_org_ids': [
            {
                'country': 'FI',
                'org_id_type': 'registration_number',
                'gov_org_id': '1234567-8',
            },
        ],
    }).json()['id']

    b = client.post('/orgs', json={
        'names': ['Company 2'],
        'gov_org_ids': [
            {
                'country': 'SE',
                'org_id_type': 'registration_number',
                'gov_org_id': '1234567-9',
            },
        ],
    }).json()['id']

    assert client.get('/orgs/search/exact/country/FI').json() == {'resources': [{'id': a}]}
    assert client.get('/orgs/search/exact/country/SE').json() == {'resources': [{'id': b}]}
    assert client.get('/orgs/search/exact/org_id_type/registration_number/exact/gov_org_id/1234567-9').json() == {
        'resources': [{'id': b}],
    }


def test_search_startswith(client, storage):
    storage.wipe_all_data('orgs')

    a = client.post('/orgs', json={'names': ['abc', 'def']}).json()['id']
    b = client.post('/orgs', json={'names': ['ghj', 'klm']}).json()['id']

    assert client.get('/orgs/search/startswith/names/ab').json() == {'resources': [{'id': a}]}
    assert client.get('/orgs/search/startswith/names/Kl').json() == {'resources': [{'id': b}]}


def test_search_contains(client, storage):
    storage.wipe_all_data('orgs')

    a = client.post('/orgs', json={'names': ['abc', 'def']}).json()['id']
    b = client.post('/orgs', json={'names': ['ghj', 'klm']}).json()['id']

    assert client.get('/orgs/search/contains/names/bc').json() == {'resources': [{'id': a}]}
    assert client.get('/orgs/search/contains/names/l').json() == {'resources': [{'id': b}]}
    assert client.get('/orgs/search/contains/names/x').json() == {'resources': []}


def test_search_gte_lte(client, storage):
    storage.wipe_all_data('test')

    a = client.post('/test', json={'string': '0', 'integer': 1, 'float': 2}).json()['id']
    b = client.post('/test', json={'string': '3', 'integer': 4, 'float': 5}).json()['id']

    assert client.get('/test/search/gt/integer/1').json() == {'resources': [{'id': b}]}
    assert client.get('/test/search/lt/integer/4').json() == {'resources': [{'id': a}]}
    assert client.get('/test/search/gt/integer/4').json() == {'resources': []}


def test_missing_resource_type(client):
    assert client.get('/invalid').status_code == 404
    assert client.get('/invalid').json() == {
        'error_code': 'ResourceTypeDoesNotExist',
        'resource_type': 'invalid',
        'message': 'Resource type does not exist',
    }
    assert client.post('/invalid', json={}).json() == {
        'error_code': 'ResourceTypeDoesNotExist',
        'resource_type': 'invalid',
        'message': 'Resource type does not exist',
    }
    assert client.get('/invalid/id').json() == {
        'error_code': 'ResourceTypeDoesNotExist',
        'resource_type': 'invalid',
        'message': 'Resource type does not exist',
    }
