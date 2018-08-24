LISTENER_1 = None
LISTENER_2 = None
LISTENER_3 = None
LISTENER_4 = None
REVISION_1 = None
REVISION_2 = None
ORG_1 = None
ORG_2 = None
MSG_1 = None
MSG_2 = None
MSG_3 = None
MSG_4 = None
MSG_5 = None
MSG_6 = None


def test_listeners_post(client, storage):
    global LISTENER_1, LISTENER_2, LISTENER_3, LISTENER_4, REVISION_1

    storage.wipe_all_data('listeners')

    client.scopes([
        'uapi_orgs_post',
        'uapi_orgs_id_put',
        'uapi_orgs_id_delete',
        'uapi_orgs_listeners_get',
        'uapi_orgs_listeners_post',
        'uapi_orgs_listeners_id_get',
        'uapi_orgs_listeners_id_put',
        'uapi_orgs_listeners_id_delete',
        'uapi_orgs_listeners_id_notifications_get',
        'uapi_orgs_listeners_id_notifications_id_get',
        'uapi_orgs_listeners_id_notifications_id_delete'
    ])

    data = {
        'listener1': {
            'notify_of_new': True
        },
        'listener2': {
            'notify_of_new': False,
        },
        'listener3': {
            'notify_of_new': False,
            'notify_on_all': True
        },
        'listener4': {
            'notify_on_all': True
        }
    }

    # Test creating new listeners
    for k, v in data.items():
        result = client.post('/orgs/listeners', json=v)
        listener = result.json()
        assert result.status_code == 201
        assert result.headers['location'].endswith(f'/orgs/listeners/{listener["id"]}')
        assert listener['type'] == 'listener'
        assert 'id' in listener
        assert 'revision' in listener

        if k != 'listener4':
            assert 'notify_of_new' in listener

        if k == 'listener1':
            LISTENER_1 = listener['id']
            assert 'notify_of_new' in listener and listener['notify_of_new'] is True
        elif k == 'listener2':
            LISTENER_2 = listener['id']
            REVISION_1 = listener['revision']
            assert 'notify_of_new' in listener and listener['notify_of_new'] is False
            assert 'listen_on' in listener and not listener['listen_on']
        elif k == 'listener3':
            LISTENER_3 = listener['id']
            assert 'notify_on_all' in listener and listener['notify_on_all'] is True
        elif k == 'listener4':
            LISTENER_4 = listener['id']


def test_listeners_get(client):
    global LISTENER_1, LISTENER_2, LISTENER_3, LISTENER_4, REVISION_1
    client.scopes([
        'uapi_orgs_post',
        'uapi_orgs_id_put',
        'uapi_orgs_id_delete',
        'uapi_orgs_listeners_get',
        'uapi_orgs_listeners_post',
        'uapi_orgs_listeners_id_get',
        'uapi_orgs_listeners_id_put',
        'uapi_orgs_listeners_id_delete',
        'uapi_orgs_listeners_id_notifications_get',
        'uapi_orgs_listeners_id_notifications_id_get',
        'uapi_orgs_listeners_id_notifications_id_delete'
    ])

    # Test if listeners are listed
    result = client.get('/orgs/listeners')
    listeners = {r['id']: None for r in result.json()['resources']}

    assert 'resources' in result.json()
    assert len(listeners.items()) >= 3
    assert result.status_code == 200
    assert LISTENER_1 in listeners
    assert LISTENER_2 in listeners
    assert LISTENER_3 in listeners
    assert LISTENER_4 in listeners


def test_listeners_id_get(client):
    global LISTENER_1, LISTENER_2, LISTENER_3, REVISION_1
    client.scopes([
        'uapi_orgs_post',
        'uapi_orgs_id_put',
        'uapi_orgs_id_delete',
        'uapi_orgs_listeners_get',
        'uapi_orgs_listeners_post',
        'uapi_orgs_listeners_id_get',
        'uapi_orgs_listeners_id_put',
        'uapi_orgs_listeners_id_delete',
        'uapi_orgs_listeners_id_notifications_get',
        'uapi_orgs_listeners_id_notifications_id_get',
        'uapi_orgs_listeners_id_notifications_id_delete'
    ])

    # Test getting listener 1
    result = client.get(f'/orgs/listeners/{LISTENER_1}')
    listener = result.json()
    assert result.status_code == 200
    assert listener == dict(
        id=LISTENER_1,
        listen_on=[],
        notify_of_new=True,
        type='listener',
        revision=listener['revision']
    )

    # Test getting listener 2
    result = client.get(f'/orgs/listeners/{LISTENER_2}')
    listener = result.json()
    assert result.status_code == 200
    assert listener == dict(
        id=LISTENER_2,
        listen_on=[],
        notify_of_new=False,
        type='listener',
        revision=REVISION_1
    )

    # Test getting listener 3
    result = client.get(f'/orgs/listeners/{LISTENER_3}')
    listener = result.json()
    assert result.status_code == 200
    assert listener == dict(
        id=LISTENER_3,
        listen_on=[],
        notify_on_all=True,
        notify_of_new=False,
        type='listener',
        revision=listener['revision']
    )

    # Test getting the correct API result code for a non existant listener
    result = client.get(f'/orgs/listeners/NON-EXISTANT')
    assert result.status_code == 404


def test_notifications_get(client):
    global LISTENER_1, REVISION_2, ORG_1, ORG_2, MSG_1, MSG_2
    client.scopes([
        'uapi_orgs_post',
        'uapi_orgs_id_put',
        'uapi_orgs_id_delete',
        'uapi_orgs_listeners_get',
        'uapi_orgs_listeners_post',
        'uapi_orgs_listeners_id_get',
        'uapi_orgs_listeners_id_put',
        'uapi_orgs_listeners_id_delete',
        'uapi_orgs_listeners_id_notifications_get',
        'uapi_orgs_listeners_id_notifications_id_get',
        'uapi_orgs_listeners_id_notifications_id_delete'
    ])

    # Test getting notifications for listener 1
    result = client.get(f'/orgs/listeners/{LISTENER_1}/notifications')
    notifications = result.json()
    assert result.status_code == 200
    assert notifications == dict(resources=[])

    # Add some new organisations and test if they got created correctly
    org1 = client.post(f'/orgs', json=dict(names=['Universal Exports'])).json()
    org2 = client.post(f'/orgs', json=dict(names=['Telebulvania Ltd'])).json()
    ORG_1 = org1['id']
    ORG_2 = org2['id']
    REVISION_2 = org1['revision']
    assert org1['names'] == ['Universal Exports']
    assert org2['names'] == ['Telebulvania Ltd']
    assert ORG_1
    assert ORG_2
    assert REVISION_2

    # Re-test if listerner 1 now has 2 notifications
    result = client.get(f'/orgs/listeners/{LISTENER_1}/notifications')
    notifications1 = result.json()
    assert result.status_code == 200
    assert len(notifications1['resources']) == 2
    MSG_1 = notifications1['resources'][0]['id']
    MSG_2 = notifications1['resources'][1]['id']


def test_notifications_id_get(client):
    global LISTENER_1, LISTENER_2, LISTENER_3, LISTENER_4, MSG_1, MSG_2, ORG_1, ORG_2
    client.scopes([
        'uapi_orgs_post',
        'uapi_orgs_id_put',
        'uapi_orgs_id_delete',
        'uapi_orgs_listeners_get',
        'uapi_orgs_listeners_post',
        'uapi_orgs_listeners_id_get',
        'uapi_orgs_listeners_id_put',
        'uapi_orgs_listeners_id_delete',
        'uapi_orgs_listeners_id_notifications_get',
        'uapi_orgs_listeners_id_notifications_id_get',
        'uapi_orgs_listeners_id_notifications_id_delete'
    ])

    # Test the 2 notifications for listener 1
    msg_result_1 = client.get(f'/orgs/listeners/{LISTENER_1}/notifications/{MSG_1}')
    msg_result_2 = client.get(f'/orgs/listeners/{LISTENER_1}/notifications/{MSG_2}')
    assert msg_result_1.status_code == 200
    assert msg_result_2.status_code == 200
    msg_result_1 = msg_result_1.json()
    msg_result_2 = msg_result_2.json()
    assert msg_result_1.get('id') == MSG_1
    assert msg_result_1.get('type') == 'notification'
    assert msg_result_1.get('resource_id') == ORG_1
    assert msg_result_1.get('resource_change') == 'created'
    assert msg_result_2.get('id') == MSG_2
    assert msg_result_2.get('type') == 'notification'
    assert msg_result_2.get('resource_id') == ORG_2
    assert msg_result_2.get('resource_change') == 'created'

    # Test if notifications of listener 2 are still 0
    result = client.get(f'/orgs/listeners/{LISTENER_2}/notifications')
    notifications2 = result.json()
    assert result.status_code == 200
    assert notifications2 == dict(resources=[])

    # Test if notifications of listener 3 are still 0
    result = client.get(f'/orgs/listeners/{LISTENER_3}/notifications')
    notifications3 = result.json()
    assert result.status_code == 200
    assert notifications3 == dict(resources=[])

    # Test if notifications of listener 4 are now 2
    result = client.get(f'/orgs/listeners/{LISTENER_4}/notifications')
    notifications4 = result.json()
    assert result.status_code == 200
    assert len(notifications4['resources']) == 2


def test_listeners_id_put(client):
    global LISTENER_2, ORG_1, REVISION_1, REVISION_2, MSG_1, MSG_2, MSG_3, MSG_4, MSG_5, MSG_6
    client.scopes([
        'uapi_orgs_post',
        'uapi_orgs_id_put',
        'uapi_orgs_id_delete',
        'uapi_orgs_listeners_get',
        'uapi_orgs_listeners_post',
        'uapi_orgs_listeners_id_get',
        'uapi_orgs_listeners_id_put',
        'uapi_orgs_listeners_id_delete',
        'uapi_orgs_listeners_id_notifications_get',
        'uapi_orgs_listeners_id_notifications_id_get',
        'uapi_orgs_listeners_id_notifications_id_delete'
    ])

    # Test updating the listener
    result = client.put(f'/orgs/listeners/{LISTENER_2}', json={
        'notify_of_new': False,
        'listen_on': [ORG_1],
        'revision': REVISION_1
    })
    assert result.status_code == 200

    # Test if it has been properly updated
    result = client.get(f'/orgs/listeners/{LISTENER_2}')
    assert result.status_code == 200
    result = result.json()
    assert result['type'] == 'listener'
    assert result['notify_of_new'] is False
    assert result['listen_on'] == [ORG_1]

    # Update the organisation 1
    org1 = client.put(f'/orgs/{ORG_1}', json=dict(names=['Universal Experts'], revision=REVISION_2))
    assert org1.status_code == 200

    # Test if listerner 2 now has 1 notification
    result = client.get(f'/orgs/listeners/{LISTENER_2}/notifications')
    notification = result.json()
    assert result.status_code == 200
    assert len(notification['resources']) == 1
    MSG_3 = notification['resources'][0]['id']

    # Test the new notifications of listener 2
    result = client.get(f'/orgs/listeners/{LISTENER_2}/notifications/{MSG_3}')
    notification = result.json()
    assert result.status_code == 200
    assert notification['id'] == MSG_3
    assert notification['type'] == 'notification'
    assert notification['resource_id'] == ORG_1
    assert notification['resource_change'] == 'updated'

    # Test if listerner 3 now has 1 notification
    result = client.get(f'/orgs/listeners/{LISTENER_3}/notifications')
    notification = result.json()
    assert result.status_code == 200
    assert len(notification['resources']) == 1
    MSG_4 = notification['resources'][0]['id']

    # Test the new notifications of listener 3
    result = client.get(f'/orgs/listeners/{LISTENER_3}/notifications/{MSG_4}')
    notification = result.json()
    assert result.status_code == 200
    assert notification['id'] == MSG_4
    assert notification['type'] == 'notification'
    assert notification['resource_id'] == ORG_1
    assert notification['resource_change'] == 'updated'

    # Test if listener 1 has no new notifications
    result = client.get(f'/orgs/listeners/{LISTENER_1}/notifications')
    notification = result.json()
    assert result.status_code == 200
    assert len(notification['resources']) == 2
    assert notification == dict(resources=[dict(id=MSG_1), dict(id=MSG_2)])

    # Delete now the organisation
    org1 = client.delete(f'/orgs/{ORG_1}')
    org2 = client.delete(f'/orgs/{ORG_1}')
    org3 = client.get(f'/orgs/{ORG_1}')
    assert org1.status_code == 200
    assert org2.status_code == 404
    assert org3.status_code == 403

    # Test if listerner 2 now has 2 notification
    result = client.get(f'/orgs/listeners/{LISTENER_2}/notifications')
    notification = result.json()
    assert result.status_code == 200
    assert len(notification['resources']) == 2
    MSG_5 = notification['resources'][1]['id']

    # Test the new notifications of listener 2
    result = client.get(f'/orgs/listeners/{LISTENER_2}/notifications/{MSG_5}')
    notification = result.json()
    assert result.status_code == 200
    assert notification['id'] == MSG_5
    assert notification['type'] == 'notification'
    assert notification['resource_id'] == ORG_1
    assert notification['resource_revision'] is None
    assert notification['resource_change'] == 'deleted'

    # Test if listerner 3 now has 2 notification
    result = client.get(f'/orgs/listeners/{LISTENER_3}/notifications')
    notification = result.json()
    assert result.status_code == 200
    assert len(notification['resources']) == 2
    MSG_6 = notification['resources'][1]['id']

    # Test the new notifications of listener 3
    result = client.get(f'/orgs/listeners/{LISTENER_3}/notifications/{MSG_6}')
    notification = result.json()
    assert result.status_code == 200
    assert notification['id'] == MSG_6
    assert notification['type'] == 'notification'
    assert notification['resource_id'] == ORG_1
    assert notification['resource_revision'] is None
    assert notification['resource_change'] == 'deleted'

    # Test if listener 1 has no new notifications
    result = client.get(f'/orgs/listeners/{LISTENER_1}/notifications')
    notification = result.json()
    assert result.status_code == 200
    assert len(notification['resources']) == 2
    assert notification == dict(resources=[dict(id=MSG_1), dict(id=MSG_2)])


def test_listeners_id_delete(client):
    global LISTENER_1, MSG_1, MSG_2
    client.scopes([
        'uapi_orgs_post',
        'uapi_orgs_id_put',
        'uapi_orgs_id_delete',
        'uapi_orgs_listeners_get',
        'uapi_orgs_listeners_post',
        'uapi_orgs_listeners_id_get',
        'uapi_orgs_listeners_id_put',
        'uapi_orgs_listeners_id_delete',
        'uapi_orgs_listeners_id_notifications_get',
        'uapi_orgs_listeners_id_notifications_id_get',
        'uapi_orgs_listeners_id_notifications_id_delete'
    ])

    org = client.delete(f'/orgs/listeners/{LISTENER_1}')
    assert org.status_code == 200

    org = client.get(f'/orgs/listeners/{LISTENER_1}')
    assert org.status_code == 404

    notifications = client.get(f'/orgs/listeners/{LISTENER_1}/notifications')
    assert notifications.status_code == 404

    notifications = client.get(f'/orgs/listeners/{LISTENER_1}/notifications/{MSG_1}')
    assert notifications.status_code == 404

    notifications = client.get(f'/orgs/listeners/{LISTENER_1}/notifications/{MSG_2}')
    assert notifications.status_code == 404


def test_notifications_id_delete(client):
    global LISTENER_2, LISTENER_3, MSG_3, MSG_4, MSG_5, MSG_6
    client.scopes([
        'uapi_orgs_post',
        'uapi_orgs_id_put',
        'uapi_orgs_id_delete',
        'uapi_orgs_listeners_get',
        'uapi_orgs_listeners_post',
        'uapi_orgs_listeners_id_get',
        'uapi_orgs_listeners_id_put',
        'uapi_orgs_listeners_id_delete',
        'uapi_orgs_listeners_id_notifications_get',
        'uapi_orgs_listeners_id_notifications_id_get',
        'uapi_orgs_listeners_id_notifications_id_delete'
    ])

    # Test deleting notifications
    result = client.delete(f'/orgs/listeners/{LISTENER_2}/notifications/{MSG_3}')
    assert result.status_code == 200

    result = client.get(f'/orgs/listeners/{LISTENER_2}/notifications/{MSG_3}')
    assert result.status_code == 404

    result = client.delete(f'/orgs/listeners/{LISTENER_3}/notifications/{MSG_4}')
    assert result.status_code == 200

    result = client.get(f'/orgs/listeners/{LISTENER_3}/notifications/{MSG_4}')
    assert result.status_code == 404

    result = client.get(f'/orgs/listeners/{LISTENER_2}/notifications/{MSG_5}')
    assert result.status_code == 200

    result = client.get(f'/orgs/listeners/{LISTENER_3}/notifications/{MSG_6}')
    assert result.status_code == 200
