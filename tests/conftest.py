import asyncio
import datetime
import pathlib

import apistar
import jwt
import pytest

from qvarn import backends
from qvarn.app import get_app


PRIVATE_KEY = b'''
-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAs4UuHySTO8lUpI2vgccHnJ520MSE9td7nOfMzL6EoMLFkUyt
AdnP/wKofHT3OKtfRO3MQcuWwDyku1otcVKViTCbOLVpKjq3Mpwbu90k32qgtc3U
ubMbeuh7TwlQJQdi0ecVa9+59buzpQuskgkYlqZSz1usVAz3axWLDrC30vZNAJd6
HUsJYjhj6AlgTmPfG1xJqVFJ8+q4XAyZ8N6ttiFjeAjcuGkbLLaWQcbbd+q8fq6O
NKyKCtpUiwPBW53G5OUAlclCL84qaAU+SZve7BQjr5ikVEbQSwd5Ck6QhBZhbOXE
R3clY9hWqfnT2jSDxF99umy+FSJxoLlTpSjXawIDAQABAoIBADg5TlwfmuM/J3zQ
CTc2jAo/0QuKePBMRaE7Mfev9Z7Z0YGzx32MZ6nA/d7YzTLY7WILrgyvRBwaAifR
Uukqib3pVLv6iSDaOdUmckMwvCMi5Il8GRM95q3kUPZMfubR/N+rpZhe/gFZ06Yt
1VL5eVN5bPcXiY3bb8QAf8hOjYwK46eRe+kGt+DFT2JhzA4+5ZWmNInDMTaS0ad8
GR76UTPsapewsR8lTH/iSvLbn7i0Ml0SF9SmuXVlylGo7YlDuDbTB9E9zecBxa0k
R8hcj7iK5C+9TnUmo5LzPB2/HZVYkXaQUN2enItDE1AVL4SJE5r/wtMGsrBT0VIy
0mBQvk0CgYEAv3Sk219UX08Y3cP3Ca0MobbLJdGdH2+n0gJOilhbiy+5Aw5W4AAF
/Uw9c/Ol4rGaEYrPHzLnNvYGpMymaXAEt2EP5B8VnLICvhDtszse5wvVzLNm5dm6
AS578YjRf1HMwEeiBTEupAhs5Cz63o9aJpAXX77t+mwxxick/pjeoncCgYEA8Ap3
BRu1vBwZ0OQkhH2FXU14jG3sZDdk6kaJxpgwUQzGxd+l/b65XfUK5lYnYBQcrrQV
c8ypwM+gpa3ZJa305oyXWL0mY8VgjVYzY3YnTrtPySFBfOTJrV9y47UbxEfBu9jb
HGyaruUf8f2SopRjcGRTnmfvauaKafUwxW8hm60CgYB9qItDSHBSFdIWS7ZqbV/r
C9SNv+RGa7xUBBuUhaWf1vSxYsn2P02vEEkNP49TGIoslVSX/4rt8dAuffuDHHib
+2K4sQY4UEWohefdSSJhNs1eiykwFxUUDXRf9RK7Y+7lDJ70lXEtTDJcGIGXbbMX
uF7/AoujXzvT+IE5cRA+xQKBgQDnlmEgJ4zd0QrDM+lbfjK2QvEkI0Wnpp4RPT7d
wKUU/UulStI+Ds2OrcT3V3Wjx6OolgwAbhv07xyh3CmdpciayleWgN9R8PLnSZxI
wIzJ9APVG+Wv1pgRUf5pXKlOZKCwWFeFg+51AJAii6/2dU2++LyIHuSpcYVnFu0X
/tEg/QKBgH9e7K1+Z+lGUgfSANeqK1mVGTXtig+ZNGp95cRbV3uimwFUaGXvNcIi
h8WwQnoA0SbIhgdvJ+TJWgTF6TbvW5Cal+93bw3OoS8Z4x9dsjLde2lcALFXGE5t
8gVLpoDECBjlZ4gBxbH2VOJJRcnR8po06IutUaC2eCvOPfEVC3cl
-----END RSA PRIVATE KEY-----
'''

PUBLIC_KEY = (
    'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCzhS4fJJM7yVSkja+Bxw'
    'ecnnbQxIT213uc58zMvoSgwsWRTK0B2c//Aqh8dPc4q19E7cxBy5bAPKS7'
    'Wi1xUpWJMJs4tWkqOrcynBu73STfaqC1zdS5sxt66HtPCVAlB2LR5xVr37'
    'n1u7OlC6ySCRiWplLPW6xUDPdrFYsOsLfS9k0Al3odSwliOGPoCWBOY98b'
    'XEmpUUnz6rhcDJnw3q22IWN4CNy4aRsstpZBxtt36rx+ro40rIoK2lSLA8'
    'Fbncbk5QCVyUIvzipoBT5Jm97sFCOvmKRURtBLB3kKTpCEFmFs5cRHdyVj'
    '2Fap+dPaNIPEX326bL4VInGguVOlKNdr'
)

SETTINGS = {
    'QVARN': {
        'BACKEND': {
            'MODULE': 'qvarn.backends.postgresql',
            'USERNAME': 'qvarn',
            'PASSWORD': 'qvarn',
            'HOST': 'localhost',
            'PORT': None,
            'DBNAME': 'planbtest',
            'INITDB': True,
        },
        'RESOURCE_TYPES_PATH': str(pathlib.Path(__file__).parent / 'resources'),
        'TOKEN_ISSUER': 'https://auth.example.org',
        'TOKEN_AUDIENCE': 'http://testserver',
        'TOKEN_SIGNING_KEY': PUBLIC_KEY,
    },
}


class TestClient(apistar.test._TestClient):

    def scopes(self, scopes):
        claims = {
            'iss': SETTINGS['QVARN']['TOKEN_ISSUER'],
            'sub': '',
            'aud': '',
            'exp': (datetime.datetime.now() + datetime.timedelta(days=10)).timestamp(),
            'scope': ' '.join(scopes),
        }
        token = jwt.encode(claims, PRIVATE_KEY, algorithm='RS512').decode()
        self.headers['Authorization'] = f'Bearer {token}'


@pytest.fixture(scope='session')
def storage():
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(backends.init(SETTINGS))


@pytest.fixture(scope='session')
def app():
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(get_app(settings=SETTINGS))


@pytest.fixture()
def client(app):
    return TestClient(app, 'http', 'testserver')
