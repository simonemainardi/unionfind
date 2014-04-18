# UnionFind

Implements the Union-Split-Find (aka disjoint-sets) data structure.

Persistence is optionally supported via MongoDB or MySQL.

## Usage
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
