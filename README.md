# UnionFind

Implements the Union-Split-Find (aka disjoint-sets) data structure.

Persistence is optionally supported via MongoDB or MySQL.

## Examples

### Standard usage with built-in python dictionaries
```
>>> from UnionFind import UnionFind
>>> family = UnionFind()
>>> family.union('mom','pop')
>>> print family['mom'], family['pop']
pop pop
>>> print family['son']
son
>>> family.union('mom','son')
>>> print family['mom'], family['pop'], family['son']
pop pop pop
>>> family.deunion('son')
>>> print family['mom'], family['pop'], family['son']
pop pop son
```

### Data consolidation (MongoDB)
```
>>> from pymongo import MongoClient
>>> client = MongoClient()  # connect
>>> db = client.a_database  # pick a database
>>> family.consolidate(db, 'uf_collection')
```

###Data consolidation (MySQL)
```
>>> import MySQLdb
>>> db = MySQLdb.connect()  # connect
>>> family.consolidate(db, 'uf_table')
```

### Usage with MongoDB persistence
```
>>> client = MongoClient()
>>> db = client.a_database
>>> family = UnionFind(db, 'uf_collection')
>>> # standard usage from now on
>>> family.union('mom', 'pop')
>>> print family['mom'], family['pop']
pop pop
```

### Usage with MySQL persistence
```
>>> db = MySQLdb.connect()
>>> family = UnionFind(db, 'uf_table', storage='mysql')
```