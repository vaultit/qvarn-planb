import importlib

from apistar import Settings


class ResourceNotFound(Exception):
    pass


class Storage:

    def add_resource_type(self, schema):
        raise NotImplemented()

    def init(self):
        raise NotImplemented()

    async def create(self, resource_path, data):
        raise NotImplemented()

    async def get(self, resource_path, row_id):
        raise NotImplemented()

    async def list(self, resource_path):
        raise NotImplemented()

    def search(self, resource_path, search_path):
        raise NotImplemented()


async def init(settings: Settings):
    backend = importlib.import_module('qvarn.backends.' + settings['QVARN']['BACKEND'])
    return await backend.init_storage(settings)


def get_storage(settings: Settings):
    return settings['storage']
