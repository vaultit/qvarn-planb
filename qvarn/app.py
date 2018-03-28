from uvicorn.run import UvicornServer

import apistar
from apistar import Command
from apistar import Component
from apistar import Include
from apistar import Route
from apistar import http
from apistar.frameworks.asyncio import ASyncIOApp
from apistar.handlers import docs_urls

from qvarn import storage
from qvarn import views
from qvarn.auth import BearerAuthentication
from qvarn.commands import token_signing_key
from qvarn.exceptions import HTTPException


class App(ASyncIOApp):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.settings = kwargs.get('settings', None)
        self.storage = None
        self.apistarapp = self

    def exception_handler(self, exc: Exception) -> http.Response:
        if isinstance(exc, HTTPException):
            return http.Response(exc.detail, status=exc.status_code, headers=exc.headers)
        else:
            return super().exception_handler(exc)


class QvarnUvicornServer(UvicornServer):

    async def create_server(self, loop, app, *args, **kwargs):
        app.apistarapp.storage = await storage.init_storage(app.apistarapp.settings)
        await super().create_server(loop, app, *args, **kwargs)


def run(app: apistar.App, host: str='127.0.0.1', port: int=8080, debug: bool=True):
    """
    Run the server.

    Args:
      app: The application instance, which should be a UMI callable.
      host: The host of the server.
      port: The port of the server.
      debug: Turn the debugger [on|off].
    """
    if debug:
        from uvitools.debug import DebugMiddleware
        apistarapp = app
        app = DebugMiddleware(apistarapp, evalex=True)
        app.apistarapp = apistarapp

    server = QvarnUvicornServer()
    server.run(app, host=host, port=port)


def init_app():
    routes = [
        Route('/auth/token', 'POST', views.auth_token),
        Route('/{resource_type}/{resource_id}', 'GET', views.resource_id_get),
        Include('/docs', docs_urls),
    ]

    commands = [
        Command('run', run),
        Command('token-signing-key', token_signing_key),
    ]

    components = [
        # Storage is preloaded in QvarnUvicornServer.create_server, immediately after uvloop is available.
        Component(storage.Storage, init=storage.get_preloaded_storage, preload=False),
    ]

    settings = {
        'AUTHENTICATION': [],
        'QVARN': {
            'RESOURCE_TYPES_PATH': '../../resources/resource-conf',
            'TOKEN_ISSUER': 'https://auth-jsonb.alpha.vaultit.org',
            'TOKEN_AUDIENCE': 'http://localhost:8080',
            'TOKEN_SIGNING_KEY': (
                'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDLDDFzdeGRZB1EOCWObzmjT34pLhLrSoU4WGu3u0IDhbaQleTQ6hTDj27DkFg20Qux8P'
                'XxcXjxzJXq+ycQDOfDP5ET+/JVeFgPxlX7aQHWyi7g5kY4LNk5AiY6/F1lD/3j4jrdMbhGDfkm44o/ow52q+mU9bnciEeISn1EjoDMH4gg'
                'k9gzZJDod6fTBwe+tkBETMMG08M9/5jgO4OE3lHBYF60EdMrSQ2kVRvUAOjmXGUyycn80g1BTAwy0SYX01MfGVgfGsSYJ/LKfbtXd5AXjm'
                'DXSC3SOsjf/9MdCZ07gNmDzqmyr4yVRJSfYdIn1Prw4BH+seVZmSQqapZ5D2hp'
            ),
        },
    }
    settings['AUTHENTICATION'] += [BearerAuthentication(settings)]

    return App(routes=routes, commands=commands, components=components, settings=settings)


def main():
    app = init_app()
    app.main()
