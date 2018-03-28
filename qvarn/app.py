import asyncio
import logging
import os
import signal

import apistar
import uvloop
from apistar import Command
from apistar import Component
from apistar import Include
from apistar import Route
from apistar import http
from apistar.frameworks.asyncio import ASyncIOApp
from apistar.handlers import docs_urls
from apistar.handlers import static_urls
from uvicorn.run import UvicornServer

from qvarn import backends
from qvarn import views
from qvarn.auth import BearerAuthentication
from qvarn.commands import token_signing_key
from qvarn.exceptions import HTTPException

logger = logging.getLogger()


class QvarnUvicornServer(UvicornServer):
    def run(self, app, host, port):
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGQUIT, self.handle_exit, signal.SIGQUIT, None)
        loop.add_signal_handler(signal.SIGTERM, self.handle_exit, signal.SIGTERM, None)
        loop.add_signal_handler(signal.SIGINT, self.handle_exit, signal.SIGINT, None)
        loop.add_signal_handler(signal.SIGABRT, self.handle_exit, signal.SIGABRT, None)
        loop.create_task(self.create_server(loop, app, host, port))
        loop.create_task(self.tick(loop))
        logger.warning('Starting worker [{}] serving at: {}:{}'.format(os.getpid(), host, port))
        loop.run_forever()


def run(app: apistar.App, host: str='127.0.0.1', port: int=8080, debug: bool=True):
    if debug:
        from uvitools.debug import DebugMiddleware
        app = DebugMiddleware(app, evalex=True)

    QvarnUvicornServer().run(app, host=host, port=port)


class App(ASyncIOApp):
    BUILTIN_COMMANDS = [
        command for command in ASyncIOApp.BUILTIN_COMMANDS if command.name != 'run'
    ] + [
        Command('run', run),
    ]

    def exception_handler(self, exc: Exception) -> http.Response:
        if isinstance(exc, HTTPException):
            return http.Response(exc.detail, status=exc.status_code, headers=exc.headers)
        else:
            return super().exception_handler(exc)


async def get_app(initdb=True):
    settings = {
        'DEBUG': True,
        'AUTHENTICATION': [],
        'QVARN': {
            'BACKEND': 'postgresql',
            'RESOURCE_TYPES_PATH': '../../resources/resource-conf',
            'TOKEN_ISSUER': 'https://auth-jsonb.alpha.vaultit.org',
            'TOKEN_AUDIENCE': 'http://localhost:8080',
            'TOKEN_SIGNING_KEY': (
                'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDLDDFzdeGRZB1EOCWObzmjT34pLhLrSoU4WGu3u0IDhbaQleTQ6hTDj27DkFg20Q'
                'ux8PXxcXjxzJXq+ycQDOfDP5ET+/JVeFgPxlX7aQHWyi7g5kY4LNk5AiY6/F1lD/3j4jrdMbhGDfkm44o/ow52q+mU9bnciEeISn1E'
                'joDMH4ggk9gzZJDod6fTBwe+tkBETMMG08M9/5jgO4OE3lHBYF60EdMrSQ2kVRvUAOjmXGUyycn80g1BTAwy0SYX01MfGVgfGsSYJ/'
                'LKfbtXd5AXjmDXSC3SOsjf/9MdCZ07gNmDzqmyr4yVRJSfYdIn1Prw4BH+seVZmSQqapZ5D2hp'
            ),
        },
    }
    settings['AUTHENTICATION'] += [BearerAuthentication(settings)]
    settings['storage'] = await backends.init(settings)

    routes = []

    if settings['DEBUG']:
        routes += [
            Include('/docs', docs_urls),
            Include('/static', static_urls),
        ]

    routes += [
        Route('/auth/token', 'POST', views.auth_token),
        Route('/{resource_type}', 'GET', views.resource_get),
        Route('/{resource_type}', 'POST', views.resource_post),
        Route('/{resource_type}/{resource_id}', 'GET', views.resource_id_get),
    ]

    commands = [
        Command('token-signing-key', token_signing_key),
    ]

    components = [
        Component(backends.Storage, init=backends.get_storage),
    ]

    return App(routes=routes, commands=commands, components=components, settings=settings)


def main():
    asyncio.get_event_loop().close()
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(get_app())
    app.main()
