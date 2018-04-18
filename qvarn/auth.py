import jwt
# import Crypto.PublicKey.RSA

from apistar import Settings
from apistar import http
from apistar.authentication import Authenticated
from apistar.interfaces import Auth
from apistar.interfaces import Router

from qvarn.exceptions import Forbidden
from qvarn.exceptions import Unauthorized


class BearerAuthentication:

    def __init__(self, settings: Settings):
        # self.pubkey = Crypto.PublicKey.RSA.importKey(settings['QVARN']['TOKEN_PUBLIC_KEY'])
        # .exportKey('OpenSSH')
        self.pubkey = settings['QVARN']['TOKEN_SIGNING_KEY']

    def authenticate(self, authorization: http.Header, settings: Settings):
        if authorization is None:
            raise Unauthorized({
                'error_code': 'AuthorizationHeaderMissing',
                'message': "Authorization header is missing",
            })

        scheme, token = authorization.split(None, 1)
        if scheme.lower() != 'bearer':
            raise Forbidden({
                'error_code': 'InvalidAuthorizationHeaderFormat',
                'message': 'Authorization header is in invalid format, should be "Bearer TOKEN"',
            })

        try:
            token = jwt.decode(token, key=self.pubkey, audience=None, options={'verify_aud': False})
        except jwt.InvalidTokenError as e:
            headers = {
                'WWW-Authenticate': 'Bearer error="invalid_token"',
            }
            raise Unauthorized({
                'error_code': 'InvalidAccessTokenError',
                'message': "Access token is invalid: {token_error}",
                'token_error': str(e),
            }, headers=headers)

        if token['iss'] != settings['QVARN']['TOKEN_ISSUER']:
            headers = {
                'WWW-Authenticate': 'Bearer error="invalid_token"',
            }
            raise Unauthorized({
                'error_code': 'InvalidAccessTokenError',
                'message': "Access token is invalid: {token_error}",
                'token_error': 'Expected issuer %s, got %s' % (settings['QVARN']['TOKEN_ISSUER'], token['iss']),
            }, headers=headers)

        if 'sub' not in token:
            headers = {
                'WWW-Authenticate': 'Bearer error="invalid_token"',
            }
            raise Unauthorized({
                'error_code': 'InvalidAccessTokenError',
                'message': "Access token is invalid: {token_error}",
                'token_error': 'Invalid subject (sub)',
            }, headers=headers)

        return Authenticated('user', token=token)


class CheckScopes:

    def __init__(self, *scopes_required):
        assert len(scopes_required) > 0
        self.scopes_required = set(scopes_required)

    def has_permission(self, auth: Auth, router: Router, path: http.Path, method: http.Method):
        if not auth.is_authenticated():
            return False

        _, kwargs = router.lookup(path, method)
        scopes_required = {scope.format(**kwargs) for scope in self.scopes_required}
        scopes_given = set(auth.token['scope'].split())
        return scopes_required <= scopes_given
