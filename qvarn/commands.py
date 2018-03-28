import base64
import urllib.parse

import requests
from Crypto.PublicKey import RSA
from Crypto.Util.number import bytes_to_long

from apistar.interfaces import Console


def _b64toint(value):
    missing_padding = '=' * (4 - len(value) % 4)
    return bytes_to_long(base64.b64decode(value + missing_padding, '-_'))


def token_signing_key(console: Console, gluu_url: str) -> None:
    """
    Get token signing key from specified Gluu server URL.

    Args:
        gluu_url: Gluu server URL.
    """
    resp = requests.get(urllib.parse.urljoin(gluu_url, 'oxauth/.well-known/openid-configuration'))
    resp = requests.get(resp.json()['jwks_uri'])
    for params in resp.json()['keys']:
        if params['alg'] == 'RS512':
            mod = _b64toint(params['n'])
            exp = _b64toint(params['e'])
            key = RSA.construct((mod, exp))
            console.echo(key.exportKey('OpenSSH').decode())
