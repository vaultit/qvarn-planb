import urllib.parse

import aiohttp
import pkg_resources as pres

from apistar import http
from apistar import Settings

from qvarn.backends import Storage
from qvarn.backends import ResourceNotFound
from qvarn.backends import WrongRevision
from qvarn.exceptions import NotFound
from qvarn.exceptions import Conflict


async def version():
    return {
        "api": {
            "version": "0.82-5.vaultit"
        },
        "implementation": {
            "name": "Qvarn PlanB",
            "version": pres.get_distribution('qvarn').version,
        }
    }


async def auth_token(headers: http.Headers, body: http.Body, settings: Settings):
    """
    Simple proxy to Gluu.

    Example:

        http -f -a user:secret post /auth/token grant_type=client_credentials scope=uapi_persons_get

    """
    async with aiohttp.ClientSession() as session:
        endpoint = urllib.parse.urljoin(settings['QVARN']['TOKEN_ISSUER'], 'oxauth/.well-known/openid-configuration')
        async with session.get(endpoint) as resp:
            config = await resp.json()
        headers = {
            'authorization': headers['authorization'],
            'content-type': headers['content-type'],
        }
        async with session.post(config['token_endpoint'], data=body, headers=headers) as resp:
            content_type = resp.headers['content-type']
            if content_type == 'application/json':
                return http.Response(await resp.json(), status=resp.status)
            else:
                return http.Response(await resp.read(), status=resp.status, content_type=content_type)


async def resource_get(resource_type, storage: Storage):
    return {
        'resources': [
            {'id': resource_id} for resource_id in await storage.list(resource_type)
        ],
    }


async def resource_post(resource_type, data: http.RequestData, storage: Storage):
    return await storage.create(resource_type, data)


async def resource_id_get(resource_type, resource_id, storage: Storage):
    try:
        return await storage.get(resource_type, resource_id)
    except ResourceNotFound:
        raise NotFound({
            'error_code': 'ItemDoesNotExist',
            'item_id': resource_id,
            'message': "Item does not exist",
        })


async def resource_id_put(resource_type, resource_id, data: http.RequestData, storage: Storage):
    try:
        return await storage.put(resource_type, resource_id, data)
    except ResourceNotFound:
        raise NotFound({
            'error_code': 'ItemDoesNotExist',
            'item_id': resource_id,
            'message': "Item does not exist",
        })
    except WrongRevision as e:
        raise Conflict({
            'error_code': 'WrongRevision',
            'item_id': resource_id,
            'current': e.current,
            'update': e.update,
            'message': (
                'Updating resource {item_id} failed: resource currently has revision {current}, update wants to '
                'update {update}.'
            ),
        })
