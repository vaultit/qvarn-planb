import importlib

from apistar import Settings


class StorageError(Exception):
    pass


class UnexpectedError(Exception):
    pass


class ResourceNotFound(StorageError):
    pass


class WrongRevision(StorageError):

    def __init__(self, message, current, update):
        super().__init__(message)
        self.current = current
        self.update = update


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
    return await get_backend_module(settings).init_storage(settings)


def get_backend_module(settings: Settings):
    return importlib.import_module(settings['QVARN']['BACKEND']['MODULE'])


def get_storage(settings: Settings):
    return settings['storage']
