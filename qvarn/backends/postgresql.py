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

from apistar import Settings

from qvarn.backends import Storage
from qvarn.backends import ResourceNotFound
from qvarn.backends import ResourceTypeNotFound
from qvarn.backends import WrongRevision
from qvarn.backends import UnexpectedError
from qvarn.validation import validated


Index = collections.namedtuple('Index', ('name', 'using', 'table', 'columns'))


def get_new_id(resource_type, random_field=None):
    type_field = hashlib.sha512(resource_type.encode()).hexdigest()[:4]
    random_field = random_field or os.urandom(16).hex()
    checksum_field = hashlib.sha512((type_field + random_field).encode()).hexdigest()[:8]
    return '{}-{}-{}'.format(type_field, random_field, checksum_field)


def chop_long_name(name, maxlen=63):
    if len(name) > maxlen:
        name_hash = hashlib.sha256(name.encode()).hexdigest()
        return name[:maxlen - 7] + '_' + name_hash[-6:]
    else:
        return name


Leaf = collections.namedtuple('Leaf', ('name', 'depth', 'inlist', 'value'))


def _flatten_for_lists(obj, key=None, depth=0, inlist=False):
    if isinstance(obj, dict):
        for k, value in sorted(obj.items(), key=operator.itemgetter(0)):
            yield from _flatten_for_lists(value, k, depth + 1, inlist)
    elif isinstance(obj, list):
        for value in obj:
            yield from _flatten_for_lists(value, key, depth + 1, inlist=True)
    elif isinstance(obj, tuple):
        for value in obj:
            yield from _flatten_for_lists(value, key, depth, inlist)
    else:
        yield Leaf(key, depth, inlist, obj)


def flatten_for_lists(obj):
    by_key = operator.attrgetter('name')
    by_depth = operator.attrgetter('depth')
    flattened = sorted(_flatten_for_lists(obj), key=by_key)
    groups = itertools.groupby(flattened, key=by_key)
    result = collections.defaultdict(dict)
    for key, values in groups:
        for i, leaf in enumerate(sorted(values, key=by_depth)):
            result[i][key] = clean_search_value(leaf.value)
    return [result[i] for i in range(len(result))]


def flatten_for_gin(obj, key=None):
    if isinstance(obj, dict):
        for k, value in obj.items():
            yield from flatten_for_gin(value, k)
    elif isinstance(obj, list):
        for value in obj:
            yield from flatten_for_gin(value, key)
    else:
        yield {key: clean_search_value(obj)}


def clean_search_value(value):
    if isinstance(value, str):
        value = value.lower()
    return value


def get_prototype_schema(prototype):
    by_key = operator.attrgetter('name')
    by_depth = operator.attrgetter('depth')
    flattened = sorted(_flatten_for_lists(prototype), key=by_key)
    groups = itertools.groupby(flattened, key=by_key)
    schema = {}
    for key, group in groups:
        group = sorted(group, key=by_depth)
        values = [leaf.value for leaf in group]
        inlist = sum(1 for leaf in group if leaf.inlist)
        schema[key] = Field(key, values, inlist)
    return schema


class Field:

    def __init__(self, name, values, inlist):
        self.name = name
        self.values = values
        self.inlist = inlist

    def search(self, value, cast=True):
        if isinstance(self.values[0], int):
            value = int(value)
        elif isinstance(self.values[0], float):
            value = float(value)
        value = clean_search_value(value)
        return sa.cast(value, JSONB) if cast else value


class PostgreSQLStorage(Storage):

    def __init__(self, engine, pool):
        self.indexes = []
        self.engine = engine
        self.pool = pool
        self.metadata = sa.MetaData(engine)
        self.inspector = reflection.Inspector.from_engine(engine)
        self.tables = {}
        self.aux_tables = collections.defaultdict(dict)
        self._resources_by_path = {}
        self.schema = {}

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
            sa.Column('search', JSONB, nullable=False),
            sa.Column('data', JSONB, nullable=False), *(
                sa.Column('data_' + subpath, JSONB, nullable=True)
                for subpath in sorted(subpaths.keys())
            )
        )
        self.tables[resource_type] = main_table

        # Define gin index for EXACT searches
        self._add_index(chop_long_name('gin_idx_' + resource_type), main_table.name, main_table.c.search)

        # Define auxiliary tables and gin indexes for all nested lists.
        aux_table = sa.Table(
            chop_long_name(resource_type + '__aux'), self.metadata,
            sa.Column('id', sa.ForeignKey(main_table.c.id, ondelete='CASCADE'), index=True),
            sa.Column('data', JSONB, nullable=False),
        )
        self.aux_tables[resource_type] = aux_table

        # Define files table if needed.
        if files:
            sa.Table(
                chop_long_name(resource_type + '__files'), self.metadata,
                sa.Column('id', sa.ForeignKey(main_table.c.id, ondelete='CASCADE'), index=True),
                sa.Column('subpath', sa.String(128), nullable=False),
                sa.Column('blob', sa.LargeBinary()),
            )

    def _get_resource_type(self, resource_path):
        try:
            return self._resources_by_path[resource_path]['type']
        except KeyError:
            raise ResourceTypeNotFound("Resource type %r not found." % resource_path)

    def _get_table(self, resource_path):
        resource_type = self._get_resource_type(resource_path)
        return self.tables[resource_type]

    def _get_subpaths(self, resource_type):
        files = set(self.schema[resource_type].get('files', []))
        return [
            subpath
            for subpath in self.schema[resource_type].get('subpaths', {}).keys()
            if subpath not in files
        ]

    def _get_prototype_schema(self, resource_type):
        subpaths = self._get_subpaths(resource_type)
        return get_prototype_schema(
            (self.schema[resource_type]['prototype'],) +
            tuple(self.schema[resource_type]['subpaths'][subpath]['prototype'] for subpath in subpaths)
        )

    async def _update_aux_tables(self, conn, resource_type, row_id, create=None):
        # TODO: should be defered

        if create is None:
            # Get data from main table and all subpaths.
            # We need this, because search look for data everywhere including all subpaths.
            table = self.tables[resource_type]
            subpaths = self._get_subpaths(resource_type)
            result = await conn.execute(sa.select(
                [table.c.data] +
                [table.c['data_' + subpath] for subpath in subpaths]
            ).where(table.c.id == row_id))
            row = await result.first()
            data = (
                [row.data] +
                [row['data_' + subpath] for subpath in subpaths if row['data_' + subpath]]
            )

            # Update search field containing data from resource and all subpaths in a convinient shape for searches.
            await conn.execute(
                table.update().
                where(table.c.id == row_id).
                values(
                    search=list(itertools.chain.from_iterable(flatten_for_gin(x) for x in data))
                )
            )

        else:
            data = create

        # Update list tables
        aux_table = self.aux_tables[resource_type]

        # Delete old rows, before inserting new ones.
        if create is None:
            await conn.execute(aux_table.delete().where(aux_table.c.id == row_id))

        # Populate aux table with data from lists, for searches.
        await conn.execute(aux_table.insert().values([{
            'id': row_id,
            'data': item,
        } for item in flatten_for_lists(data)]))

    def add_resource_type(self, schema):
        self.schema[schema['type']] = schema['versions'][-1]
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

        data = validated(resource_type, self.schema[resource_type]['prototype'], data)
        search = list(flatten_for_gin(data))

        async with self.pool.acquire() as conn:
            async with conn.begin():
                await conn.execute(table.insert().values(id=row_id, revision=revision, data=data, search=search))
                await self._update_aux_tables(conn, resource_type, row_id, data)

        return dict(data, id=row_id, revision=revision)

    async def get(self, resource_path, row_id):
        table = self._get_table(resource_path)
        async with self.pool.acquire() as conn:
            result = await conn.execute(sa.select([
                table.c.id,
                table.c.revision,
                table.c.data,
            ]).where(table.c.id == row_id))
            row = await result.first()
        if row:
            return dict(row.data, id=row.id, revision=row.revision)
        else:
            raise ResourceNotFound("Resource %s not found." % row_id)

    async def put(self, resource_path, row_id, data):
        table = self._get_table(resource_path)

        resource_type = self._get_resource_type(resource_path)
        new_revision = get_new_id(resource_type)
        old_revision = data.get('revision')

        data = validated(resource_type, self.schema[resource_type]['prototype'], data)
        search = list(flatten_for_gin(data))

        async with self.pool.acquire() as conn:
            async with conn.begin():
                result = await conn.execute(
                    table.update().
                    where(table.c.id == row_id).
                    where(table.c.revision == old_revision).
                    values(revision=new_revision, data=data, search=search)
                )

                if result.rowcount == 1:
                    await self._update_aux_tables(conn, resource_type, row_id)

                elif result.rowcount == 0:
                    result = await conn.execute(sa.select([table.c.revision]).where(table.c.id == row_id))
                    row = await result.first()
                    if row is None:
                        raise ResourceNotFound("Resource %s not found." % row_id)
                    else:
                        raise WrongRevision("Expected revision is %s, got %s." % (row.revision, old_revision),
                                            current=row.revision, update=old_revision)

                else:
                    raise UnexpectedError((
                        "Update query returned %r rowcount, expected values are 0 or 1. Don't know how to handle that."
                    ) % result.rowcount)

        return dict(data, id=row_id, revision=new_revision)

    async def delete(self, resource_path, row_id):
        resource_type = self._get_resource_type(resource_path)
        table = self._get_table(resource_path)
        aux_table = self.aux_tables[resource_type]

        with self.engine.begin() as conn:
            conn.execute(table.delete().where(table.c.id == row_id))
            conn.execute(aux_table.delete().where(table.c.id == row_id))

        return {}

    async def get_subpath(self, resource_path, row_id, subpath):
        table = self._get_table(resource_path)
        async with self.pool.acquire() as conn:
            result = await conn.execute(sa.select([
                table.c.revision,
                table.c['data_' + subpath],
            ]).where(table.c.id == row_id))
            row = await result.first()
        if row:
            return dict(row['data_' + subpath], revision=row.revision)
        else:
            raise ResourceNotFound("Resource %s not found." % row_id)

    async def put_subpath(self, resource_path, row_id, subpath, data):
        table = self._get_table(resource_path)

        resource_type = self._get_resource_type(resource_path)
        new_revision = get_new_id(resource_type)
        old_revision = data.get('revision')

        data = validated(resource_type, self.schema[resource_type]['subpaths'][subpath]['prototype'], data)

        async with self.pool.acquire() as conn:
            async with conn.begin():
                result = await conn.execute(
                    table.update().
                    where(table.c.id == row_id).
                    where(table.c.revision == old_revision).
                    values({
                        'revision': new_revision,
                        'data_' + subpath: data,
                    })
                )

                if result.rowcount == 1:
                    await self._update_aux_tables(conn, resource_type, row_id)

                elif result.rowcount == 0:
                    result = await conn.execute(sa.select([table.c.revision]).where(table.c.id == row_id))
                    row = await result.first()
                    if row is None:
                        raise ResourceNotFound("Resource %s not found." % row_id)
                    else:
                        raise WrongRevision("Expected revision is %s, got %s." % (row.revision, old_revision),
                                            current=row.revision, update=old_revision)

                else:
                    raise UnexpectedError((
                        "Update query returned %r rowcount, expected values are 0 or 1. Don't know how to handle that."
                    ) % result.rowcount)

        return dict(data, revision=new_revision)

    async def list(self, resource_path):
        table = self._get_table(resource_path)
        async with self.pool.acquire() as conn:
            return [
                row.id async for row in conn.execute(
                    sa.select([table.c.id])
                )
            ]

    async def search(self, resource_path, search_path):
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
            operator = next(words, None)

        sort_keys = []
        show_all = False
        show = []
        offset = None
        limit = None
        where = []
        gin = []
        joins = []

        table = self._get_table(resource_path)
        resource_type = self._get_resource_type(resource_path)
        aux_table = self.aux_tables[resource_type]
        schema = self._get_prototype_schema(resource_type)

        for operator, args in operators:

            if operator == 'show_all':
                show_all = True

            elif operator == 'show':
                show.extend(args)

            elif operator == 'sort':
                sort_keys.extend(args)

            elif operator == 'offset':
                offset = int(args[0])

            elif operator == 'limit':
                limit = int(args[0])

            elif operator == 'exact':
                key, value = args
                value = schema[key].search(value, cast=False)
                gin.append({key: value})

            elif operator == 'startswith':
                key, value = args
                value = schema[key].search(value, cast=False)
                alias = aux_table.alias('t' + str(len(joins) + 1))
                joins.append(table.join(alias, table.c.id == alias.c.id))
                where.append(alias.c.data[key].astext.startswith(value))

            elif operator == 'contains':
                key, value = args
                value = schema[key].search(value, cast=False)
                alias = aux_table.alias('t' + str(len(joins) + 1))
                joins.append(table.join(alias, table.c.id == alias.c.id))
                where.append(alias.c.data[key].astext.contains(value))

            elif operator == 'ge':
                key, value = args
                value = schema[key].search(value)
                alias = aux_table.alias('t' + str(len(joins) + 1))
                joins.append(table.join(alias, table.c.id == alias.c.id))
                where.append(alias.c.data[key] >= value)

            elif operator == 'gt':
                key, value = args
                value = schema[key].search(value)
                alias = aux_table.alias('t' + str(len(joins) + 1))
                joins.append(table.join(alias, table.c.id == alias.c.id))
                where.append(alias.c.data[key] > value)

            elif operator == 'le':
                key, value = args
                value = schema[key].search(value)
                alias = aux_table.alias('t' + str(len(joins) + 1))
                joins.append(table.join(alias, table.c.id == alias.c.id))
                where.append(alias.c.data[key] <= value)

            elif operator == 'lt':
                key, value = args
                value = schema[key].search(value)
                alias = aux_table.alias('t' + str(len(joins) + 1))
                joins.append(table.join(alias, table.c.id == alias.c.id))
                where.append(alias.c.data[key] < value)

            elif operator == 'ne':
                key, value = args
                value = schema[key].search(value)
                alias = aux_table.alias('t' + str(len(joins) + 1))
                joins.append(table.join(alias, table.c.id == alias.c.id))
                where.append(alias.c.data[key] != value)

            else:
                raise Exception("Operator %r is not yet implemented." % operator)

        if show_all is False and len(show) == 0:
            query = sa.select([table.c.id], distinct=table.c.id)
        else:
            query = sa.select([table.c.id, table.c.revision, table.c.data], distinct=table.c.id)

        for join in joins:
            query = query.select_from(join)

        if gin:
            where.append(table.c.search.contains(gin))

        if where:
            query = query.where(sa.and_(*where))

        if sort_keys:
            db_sort_keys = []
            for sort_key in sort_keys:
                if sort_key == 'id':
                    db_sort_keys.append(table.c.id)
                else:
                    db_sort_keys.append(table.c.data[sort_key])
            query = query.order_by(*db_sort_keys)

        if limit:
            query = query.limit(limit)

        if offset:
            query = query.offset(offset)

        async with self.pool.acquire() as conn:
            result = conn.execute(query)

            if show_all:
                return [dict(row.data, id=row.id, revision=row.revision) async for row in result]
            elif show:
                return [dict({field: row.data[field] for field in show if field in row.data}, id=row.id) async for row in result]
            else:
                return [{'id': row.id} async for row in result]

    def wipe_all_data(self, *resource_paths):
        """A quick way to wipe all data in specified resource paths, mainly used for tests."""
        with self.engine.begin() as conn:
            for resource_path in resource_paths:
                table = self._get_table(resource_path)
                resource_type = self._get_resource_type(resource_path)
                conn.execute(table.delete())
                aux_table = self.aux_tables[resource_type]
                conn.execute(aux_table.delete())


def settings_to_dsn(settings):
    dsn = 'postgresql://'
    if settings['USERNAME']:
        dsn += '%s:%s@' % (settings['USERNAME'], settings['PASSWORD'])
    if settings['HOST']:
        dsn += settings['HOST']
        if settings['PORT']:
            dsn += ':' + settings['PORT']
    dsn += '/' + settings['DBNAME']
    return dsn


async def init_storage(settings: Settings):
    dsn = settings_to_dsn(settings['QVARN']['BACKEND'])
    engine = sa.create_engine(dsn, echo=False)
    pool = await aiopg.sa.create_engine(dsn)
    storage = PostgreSQLStorage(engine, pool)

    resource_types_path = pathlib.Path(settings['QVARN']['RESOURCE_TYPES_PATH'])
    if not resource_types_path.exists():
        raise Exception('RESOURCE_TYPES_PATH not found: ' + settings['QVARN']['RESOURCE_TYPES_PATH'])

    for path in sorted(resource_types_path.glob('*.yaml')):
        schema = yaml.safe_load(path.read_text())
        storage.add_resource_type(schema)

    if settings['QVARN']['BACKEND']['INITDB']:
        storage.init()

    return storage
