import urllib.parse

import aiohttp
import pkg_resources as pres

from apistar import annotate
from apistar import http
from apistar import Response
from apistar import Settings
from apistar.types import PathWildcard
from apistar.parsers import JSONParser

from qvarn.backends import Storage
from qvarn.backends import ResourceNotFound
from qvarn.backends import ResourceTypeNotFound
from qvarn.backends import WrongRevision
from qvarn.exceptions import NotFound
from qvarn.exceptions import Conflict
from qvarn.auth import CheckScopes


@annotate(
    permissions=[CheckScopes('uapi_{resource_type}_listeners_get')],
)
async def listeners_get(resource_type: str, storage: Storage):
    try:
        return {
            'resources': [
                {'id': listener_id} for listener_id in await storage.list_listeners(resource_type)
            ],
        }
    except ResourceTypeNotFound:
        raise NotFound({
            'error_code': 'ResourceTypeDoesNotExist',
            'resource_type': resource_type + '/listeners',
            'message': 'Resource type does not exist',
        })


@annotate(
    permissions=[CheckScopes('uapi_{resource_type}_listeners_post')],
)
async def listeners_post(resource_type: str, data: http.RequestData, storage: Storage):
    try:
        return await storage.create_listener(resource_type, data)
    except ResourceTypeNotFound:
        raise NotFound({
            'error_code': 'ResourceTypeDoesNotExist',
            'resource_type': resource_type + '/listeners',
            'message': 'Resource type does not exist',
        })


@annotate(
    permissions=[CheckScopes('uapi_{resource_type}_listeners_id_get')],
)
async def listeners_id_get(resource_type: str, listener_id: str, storage: Storage):
    try:
        return await storage.get_listener(resource_type, listener_id)
    except ResourceTypeNotFound:
        raise NotFound({
            'error_code': 'ResourceTypeDoesNotExist',
            'resource_type': resource_type + '/listeners',
            'message': 'Resource type does not exist',
        })
    except ResourceNotFound:
        raise NotFound({
            'error_code': 'ItemDoesNotExist',
            'item_id': listener_id,
            'message': "Item does not exist",
        })


@annotate(
    permissions=[CheckScopes('uapi_{resource_type}_listeners_id_put')],
)
async def listeners_id_put(resource_type: str, listener_id: str, data: http.RequestData, storage: Storage):
    try:
        return await storage.put_listener(resource_type, listener_id, data)
    except ResourceTypeNotFound:
        raise NotFound({
            'error_code': 'ResourceTypeDoesNotExist',
            'resource_type': resource_type + '/listeners',
            'message': 'Resource type does not exist',
        })
    except ResourceNotFound:
        raise NotFound({
            'error_code': 'ItemDoesNotExist',
            'item_id': listener_id,
            'message': "Item does not exist",
        })
    except WrongRevision as e:
        raise Conflict({
            'error_code': 'WrongRevision',
            'item_id': listener_id,
            'current': e.current,
            'update': e.update,
            'message': (
                'Updating resource {item_id} failed: resource currently has revision {current}, update wants to '
                'update {update}.'
            ),
        })


@annotate(
    permissions=[CheckScopes('uapi_{resource_type}_listeners_id_delete')],
)
async def listeners_id_delete(resource_type: str, listener_id: str, storage: Storage):
    try:
        return await storage.delete_listener(resource_type, listener_id)
    except ResourceTypeNotFound:
        raise NotFound({
            'error_code': 'ResourceTypeDoesNotExist',
            'resource_type': resource_type + '/listeners',
            'message': 'Resource type does not exist',
        })
    except ResourceNotFound:
        raise NotFound({
            'error_code': 'ItemDoesNotExist',
            'item_id': listener_id,
            'message': "Item does not exist",
        })
    except WrongRevision as e:
        raise Conflict({
            'error_code': 'WrongRevision',
            'item_id': listener_id,
            'current': e.current,
            'update': e.update,
            'message': (
                'Updating resource {item_id} failed: resource currently has revision {current}, update wants to '
                'update {update}.'
            ),
        })


@annotate(
    permissions=[CheckScopes('uapi_{resource_type}_listeners_id_notifications_get')],
)
async def notifications_get(resource_type: str, listener_id: str, storage: Storage):
    try:
        return {
            'resources': [
                {'id': notification_id} for notification_id in await storage.list_notifications(resource_type, listener_id)
            ],
        }
    except ResourceTypeNotFound:
        raise NotFound({
            'error_code': 'ResourceTypeDoesNotExist',
            'resource_type': f'{resource_type}/listeners/{listener_id}/notifications',
            'message': 'Resource type does not exist',
        })


@annotate(
    permissions=[CheckScopes('uapi_{resource_type}_listeners_id_notifications_id_get')],
)
async def notifications_id_get(resource_type: str, listener_id: str, notification_id: str, storage: Storage):
    try:
        return await storage.get_notification(resource_type, listener_id, notification_id)
    except ResourceTypeNotFound:
        raise NotFound({
            'error_code': 'ResourceTypeDoesNotExist',
            'resource_type': f'{resource_type}/listeners/{listener_id}/notifications',
            'message': 'Resource type does not exist',
        })
    except ResourceNotFound:
        raise NotFound({
            'error_code': 'ItemDoesNotExist',
            'listener_id': listener_id,
            'item_id': notificaiton_id,
            'message': "Item does not exist",
        })


@annotate(
    permissions=[CheckScopes('uapi_{resource_type}_listeners_id_notifications_id_delete')],
)
async def notifications_id_delete(resource_type, listener_id, notification_id, storage: Storage):
    try:
        return await storage.delete_notification(resource_type, listener_id, notification_id)
    except ResourceTypeNotFound:
        raise NotFound({
            'error_code': 'ResourceTypeDoesNotExist',
            'resource_type': f'{resource_type}/listeners/{listener_id}/notifications',
            'message': 'Resource type does not exist',
        })
    except ResourceNotFound:
        raise NotFound({
            'error_code': 'ItemDoesNotExist',
            'listener_id': listener_id,
            'item_id': notification_id,
            'message': "Item does not exist",
        })
    except WrongRevision as e:
        raise Conflict({
            'error_code': 'WrongRevision',
            'listener_id': listener_id,
            'item_id': notification_id,
            'current': e.current,
            'update': e.update,
            'message': (
                'Updating resource {item_id} failed: resource currently has revision {current}, update wants to '
                'update {update}.'
            ),
        })
