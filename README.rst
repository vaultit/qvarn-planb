This is an experimental QvarnAPI implementation focused on performance.

Everything is build on `API Star`_ web framework, using asyncio and Python 3.6.
All code is written from scratch.

.. _API Star: http://www.encode.io/apistar/

Implemented Qvarn API endpoints:

- ``GET    /version``
- ``GET    /{resource_type}``
- ``POST   /{resource_type}``
- ``GET    /{resource_type}/search/{query}``
- ``GET    /{resource_type}/{resource_id}``
- ``PUT    /{resource_type}/{resource_id}``
- ``DELETE /{resource_type}/{resource_id}``
- ``GET    /{resource_type}/{resource_id}/{subpath}``
- ``PUT    /{resource_type}/{resource_id}/{subpath}``

Authorization is implemented, but endpoint handlers are not using it yet.


How to run it?
==============

Create a database and database user::

  > sudo -u postgres psql
  # CREATE DATABASE planb;
  # GRANT ALL PRIVILEGES ON DATABASE planb TO qvarn;

Change database connection parameters and ``RESOURCE_TYPES_PATH`` in
``qvarn/app.py`` file.

Run the server::

  > make run
  env/bin/qvarn run --host 0.0.0.0
  Starting worker [24766] serving at: 0.0.0.0:8000

Test if it works::

  > http -b get :8000/version
  {
      "api": {
          "version": "0.82-5.vaultit"
      },
      "implementation": {
          "name": "Qvarn PlanB",
          "version": "0.0.2.dev3+gf0224a5.d20180329"
      }
  }


Database structure
==================

Data are stored in PostgreSQL database.

In the example about haw data are stored in PostgreSQL I will use following
example resource:

.. code-block:: json

  {
    "a": 1,
    "b": [2, 3],
    "c": [
      {"d": 4},
    ],
    "d": 5,
    "e": {
      "f": 6,
    },
  }


Tables
------

For each Qvarn resource type, three PostgreSQL tables are created:

- ``resource_type`` - main table, stores whole resource, subresources and a
  structure transformed for GIN indexes. Subresources are stored in separate
  fields.

- ``resource_type__aux`` - a table used for searches, where GIN indexes can't
  be used.

- ``resource_type__files``


Exact searches
--------------

For EXACT searches PostgreSQL has `GIN indexes`_. In order to be able to use
GIN index, resource data is transformed into this:

.. _GIN indexes: https://www.postgresql.org/docs/9.6/static/gin.html

.. code-block:: json

  [
    {"a": 1},
    {"b": 2},
    {"b": 3},
    {"d": 4},
    {"d": 5},
    {"f": 6},
  ]

Whith this structure, we can run any EXACT search with a single condition:

.. code-block:: python

  table.c.search.contains([
      {'d': 4},
      {'d': 5},
      {'a': 2},
  ])


Non-exact searches
------------------

For all non-EXACT searches, ``__aux`` table is used. Resource data in this
table is transformed like this:


.. code-block:: json

  {"a": 1, "b": 2, "d": 5, "f": 6}

  {        "b": 3, "d": 4}

Here resource data tree is traversed in breadth-first order and all non
repeated keys are witen to ``__aux`` table as separate row.

This way, amount of rows in ``__aux`` table is minimized and there is a
possibility to add indexed on each separate field to boost performance.

The query is constructed this way:

.. code-block:: python

  aux = aux_table.alias('t1')

  query = (
      sa.select([table.c.id], distinct=table.c.id).
      select_from(table.join(aux, table.c.id == aux.c.id)).
      aux.c.data[key].astext.startswith(value)
  )

Each non-exact search criteria requires a join.
