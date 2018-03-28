import aiopg.sa
import collections
import hashlib
import itertools
import operator
import os
import pathlib
import urllib.parse

import ruamel.yaml as yaml
import sqlalchemy as sa
from sqlalchemy.engine import reflection
from sqlalchemy.dialects.postgresql import JSONB

from apistar import App
from apistar import Settings


Index = collections.namedtuple('Index', ('name', 'using', 'table', 'columns'))


class ResourceNotFound(Exception):
    pass


def get_new_id(resource_type, random_field=None):
    type_field = hashlib.sha512(resource_type.encode()).hexdigest()[:4]
    random_field = random_field or os.urandom(16).hex()
    checksum_field = hashlib.sha512((type_field + random_field).encode()).hexdigest()[:8]
    return '{}-{}-{}'.format(type_field, random_field, checksum_field)


def strip_lists(data):
    if isinstance(data, dict):
        return {k: strip_lists(v) for k, v in data.items() if not isinstance(v, list)}
    else:
        return data


def _separate_lists(data, path=()):
    if isinstance(data, dict):
        for k, v in data.items():
            yield from _separate_lists(v, path + (k,))
    elif isinstance(data, list):
        for i, v in enumerate(data):
            if not isinstance(v, list):
                yield path, strip_lists(v)
            yield from _separate_lists(v, path)


def iter_lists(data):
    sort_key = operator.itemgetter(0)
    data = sorted(_separate_lists(data), key=sort_key)
    for k, g in itertools.groupby(data, key=sort_key):
        yield k, [v for _, v in g]


def chop_long_name(name, maxlen=63):
    if len(name) > maxlen:
        name_hash = hashlib.sha256(name.encode()).hexdigest()
        return name[:maxlen - 7] + '_' + name_hash[-6:]
    else:
        return name


class Storage:

    def __init__(self, engine, pool):
        self.indexes = []
        self.engine = engine
        self.pool = pool
        self.metadata = sa.MetaData(engine)
        self.inspector = reflection.Inspector.from_engine(engine)
        self.tables = {}
        self.aux_tables = collections.defaultdict(dict)
        self._resources_by_path = {}

    def _add_index(self, name, table, *columns, using='gin'):
        self.indexes.append(Index(name, using, table, columns))

    def _create_indexes(self):
        self.inspector = reflection.Inspector.from_engine(self.engine)
        existing_tables = self.inspector.get_table_names()
        sort_key = operator.attrgetter('table')
        indexes = sorted(self.indexes, key=sort_key)
        indexes = itertools.groupby(indexes, key=sort_key)
        for table, table_indexes in indexes:
            if table in existing_tables:
                existing_indexes = {x['name'] for x in self.inspector.get_indexes(table)}
                for index in table_indexes:
                    if index.name not in existing_indexes:
                        if index.using == 'gin':
                            idx = sa.Index(
                                index.name, *index.columns,
                                postgresql_using='gin',
                                postgresql_ops={'data': 'jsonb_path_ops'},
                            )
                        else:
                            raise Exception(
                                "Unknown index 'using' paramter: %r." %
                                index.using
                            )
                        idx.create()

    def _create_aux_tables(self, main_table, resource_type, subpath,
                           prototype):
        for k, v in iter_lists(prototype):
            if subpath:
                suffix = '__sub__' + subpath + '__aux__' + '__'.join(k)
            else:
                suffix = '__aux__' + '__'.join(k)
            aux_table_name = resource_type + suffix
            aux_table = sa.Table(
                chop_long_name(aux_table_name), self.metadata,
                sa.Column('id', sa.ForeignKey(main_table.c.id, ondelete='CASCADE')),
                sa.Column('subpath', sa.String(128), nullable=False),
                sa.Column('data', JSONB, nullable=False),
            )
            if subpath:
                self.aux_tables[(resource_type, subpath)][k] = aux_table
            else:
                self.aux_tables[resource_type][k] = aux_table
            aux_index_name = 'gin_idx_' + resource_type + suffix
            self._add_index(chop_long_name(aux_index_name), aux_table_name, aux_table.c.data)

    def _create_tables(self, schema):
        version = schema['versions'][-1]
        subpaths = version.get('subpaths', {})
        resource_type = schema['type']
        files = version.get('files', [])

        # Define main table
        main_table = sa.Table(
            chop_long_name(resource_type), self.metadata,
            sa.Column('id', sa.String(46), primary_key=True),
            sa.Column('revision', sa.String(46)),
            sa.Column('data', JSONB, nullable=False), *(
                sa.Column('data_' + subpath, JSONB, nullable=True)
                for subpath in sorted(subpaths.keys())
            )
        )
        self.tables[resource_type] = main_table

        # Define gin index for main table
        self._add_index(chop_long_name('gin_idx_' + resource_type), main_table.name, main_table.c.data)

        # Define gin indexes for all subpaths in main table
        for subpath in sorted(subpaths.keys()):
            subpath_index_name = 'gin_idx_' + resource_type + '__sub__' + subpath
            self._add_index(chop_long_name(subpath_index_name), main_table.name, main_table.c['data_' + subpath])

        # Define files table if needed.
        if files:
            sa.Table(
                chop_long_name(resource_type + '__files'), self.metadata,
                sa.Column('id', sa.ForeignKey(main_table.c.id, ondelete='CASCADE')),
                sa.Column('subpath', sa.String(128), nullable=False),
                sa.Column('blob', sa.LargeBinary()),
            )

        # Define auxiliary tables and gin indexes for all nested lists.
        self._create_aux_tables(main_table, resource_type, '', version['prototype'])
        for subpath, subpath_schema in version.get('subpaths', {}).items():
            if subpath not in files:
                self._create_aux_tables(main_table, resource_type, subpath, subpath_schema['prototype'])

    def _get_resource_type(self, resource_path):
        return self._resources_by_path[resource_path]['type']

    def _get_table(self, resource_path):
        resource_type = self._get_resource_type(resource_path)
        return self.tables[resource_type]

    def add_resource_type(self, schema):
        self._create_tables(schema)
        self._resources_by_path[schema['path'].strip('/')] = schema

    def init(self):
        self.metadata.create_all()
        self._create_indexes()

    async def create(self, resource_path, data):
        resource_type = self._get_resource_type(resource_path)
        table = self._get_table(resource_path)

        row_id = get_new_id(resource_type)
        revision = get_new_id(resource_type)

        async with self.pool.acquire() as conn:
            async with conn.begin():
                await conn.execute(table.insert().values(id=row_id, revision=revision, data=data))
                for path, items in iter_lists(data):
                    aux_table = self.aux_tables[resource_type][path]
                    await conn.execute(aux_table.insert().values([{
                        'id': row_id,
                        'subpath': '',
                        'data': item,
                    } for item in items]))
        return row_id

    async def get(self, resource_path, row_id):
        table = self._get_table(resource_path)
        async with self.pool.acquire() as conn:
            result = await conn.execute(sa.select([table]).where(table.c.id == row_id))
            row = await result.first()
        if row:
            return dict(row.data, id=row.id, revision=row.revision)
        else:
            raise ResourceNotFound()

    async def list(self, resource_path):
        table = self._get_table(resource_path)
        async with self.pool.acquire() as conn:
            return [
                row.id async for row in conn.execute(
                    sa.select([table.c.id])
                )
            ]

    def search(self, resource_path, search_path):
        operator_args = {
            'contains': 2,
            'exact': 2,
            'ge': 2,
            'gt': 2,
            'le': 2,
            'lt': 2,
            'ne': 2,
            'startswith': 2,
            'show': 1,
            'show_all': 0,
            'sort': 1,
            'offset': 1,
            'limit': 1,
        }
        operators = []
        words = map(urllib.parse.unquote, search_path.split('/'))
        operator = next(words, None)
        while operator:
            if operator not in operator_args:
                raise Exception("Unknown operator %r." % operator)
            args_count = operator_args[operator]
            try:
                args = [next(words) for i in range(args_count)]
            except StopIteration:
                raise Exception("Operator %r requires at least %d arguments." % (operator, args_count))
            operators.append((operator, args))

        fields = []
        sort_keys = []
        show_all = False
        offset = None
        limit = None

        table = self._get_table(resource_path)

        for operator, args in operators:
            if operator == 'show_all':
                show_all = True
            elif operator == 'show':
                fields.extend(args)
            elif operator == 'sort':
                sort_keys.extend(args)
            elif operator == 'offset':
                offset = int(args[0])
            elif operator == 'limit':
                limit = int(args[0])
            else:
                raise Exception("Operator %r is not yet implemented." % operator)

        if show_all is False and len(fields) == 0:
            query = sa.select([table.c.id])
        else:
            query = sa.select([table.c.id, table.c.revision, table.c.body])

        if sort_keys:
            query = query.order_by(*(table.c[key] for key in sort_keys))

        if limit:
            query = query.limit(limit)

        if offset:
            query = query.offset(offset)

        result = self.engine.execute(query)

        if show_all:
            return [dict(row.data, id=row.id, revision=row.revision) for row in result]
        elif fields:
            return [dict({field: row[field] for field in fields}, id=row.id, revision=row.revision) for row in result]
        else:
            return [dict(id=row.id, revision=row.revision) for row in result]


async def init_storage(settings: Settings):
    engine = sa.create_engine('postgresql:///planb', echo=False)
    pool = await aiopg.sa.create_engine(database='planb')
    storage = Storage(engine, pool)
    for path in sorted(pathlib.Path(settings['QVARN']['RESOURCE_TYPES_PATH']).glob('*.yaml')):
        schema = yaml.safe_load(path.read_text())
        storage.add_resource_type(schema)
    return storage


def get_preloaded_storage(app: App):
    return app.storage
