"""UnionFind.py
Python implementation of disjoint sets data structures.

Changes to include mongodb persistence for disjoint sets included by Simone Mainardi.

Original Union-find data structure based on Josiah Carlson's code,
http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/215912
with significant additional changes by D. Eppstein.

"""
import abc
try:
    import pymongo
    import MySQLdb
except ImportError:
    # we can still use union-find with standard python dictionaries
    pass

from warnings import filterwarnings
filterwarnings('ignore', category=MySQLdb.Warning)

available_storage = ['mongodb', 'mysql']


class Parents(object):
    """
    Abstract class to define the interface of disjoint sets objects
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __contains__(self, obj):
        """ Return true if the object `obj` is present in the disjoint sets. """
        return

    @abc.abstractmethod
    def __getitem__(self, obj):
        """ Return the object `obj` if it is present in the disjoint sets. """
        return

    @abc.abstractmethod
    def __setitem__(self, obj, parent):
        """ Add the object `obj` to the disjoint sets with weight 1 if not present.
        Then, update the parent member of the object `obj` with the argument `parent`.
        """
        return

    @abc.abstractmethod
    def inc_weight(self, obj, weight):
        """ Increment the weight of the object `obj` by the value of the argument `weight`. """
        return

    @abc.abstractmethod
    def items(self):
        """ Return the objects in the disjoint sets as a list of 2-tuples `(object, parent_object)` """
        return


class MySQLParents(Parents):
    """
    Handle disjoint sets, via mysql.
    """
    def __init__(self, db, table=None):
        """
        Parameters:
        -----------
        :param db: an instance of MySQLdb.connections.Connection or None
        :param table: a string representing the table in the db or None
        """
        if not isinstance(db, MySQLdb.connections.Connection):
            raise TypeError('db must be a valid instance of MySQLdb.connections.Connection')

        self.db = db
        self.cur = db.cursor(MySQLdb.cursors.DictCursor)
        self.table = table

    def __contains__(self, obj):
        query = " SELECT * FROM %s " % self.table
        query += " WHERE _id = %s "  # this time we let mysql replace the %s
        self.cur.execute(query, (obj,))
        return self.cur.rowcount > 0

    def __getitem__(self, obj):
        query = " SELECT * FROM %s " % self.table
        query += " WHERE _id = %s "  # this time we let mysql replace the %s
        self.cur.execute(query, (obj,))
        return self.cur.fetchone()

    def __setitem__(self, obj, parent):
        query = " SELECT * FROM %s " % self.table
        query += " WHERE _id = %s "  # this time we let mysql replace the %s
        self.cur.execute(query, (obj,))
        obj_el = self.cur.fetchone()
        if obj_el is None:  # there wasn't any row with column _id equal to key in the database!
            # ignore the parent !
            obj_el = {'_id': obj, 'parent': obj, 'weight': 1}
        else:  # there is already an entry with _id equal to key!
            query = " SELECT * FROM %s " % self.table
            query += " WHERE _id = %s "  # this time we let mysql replace the %s
            self.cur.execute(query, (parent,))
            parent_el = self.cur.fetchone()
            obj_el['parent'] = parent_el['_id']
        with self.db:
            # simulate an UPSERT
            query = " INSERT INTO %s " % self.table
            query += " VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE parent = %s"
            self.cur.execute(query, (obj_el['_id'], obj_el['parent'], obj_el['weight'], obj_el['parent']))

    def inc_weight(self, obj, weight):
        query = " UPDATE %s " % self.table
        query += "SET weight = weight + %s WHERE _id = %s"
        with self.db:
            self.cur.execute(query, (weight, obj))

    def items(self):
        self.cur.execute(" SELECT * from %s " % self.table)
        res = {el.pop('_id'): el for el in self.cur.fetchall()}
        for el in res.items():
            yield el


class MongoParents(Parents):
    """
    Handle disjoint sets, via mongodb.

    The user that is interested in union-find data structures, should not
    use this class directly. Indeed, the class UnionFind already implements
    union-find features.
    """
    def __init__(self, db, collection=None):
        """
        Parameters:
        -----------
        :param db: an instance of pymongo.database.Database or None
        :param collection: a string representing the collection in the db or None
        """
        if not isinstance(db, pymongo.database.Database):
            raise TypeError('db must be a valid instance of pymongo.database.Database')

        self.db = db
        self.collection = collection

    def __contains__(self, obj):
        return self.db[self.collection].find({'_id': obj}, {'_id': 1}).count() > 0

    def __getitem__(self, obj):
        return self.db[self.collection].find_one({'_id': obj})

    def __setitem__(self, obj, parent):
        obj_el = self.db[self.collection].find_one({'_id': obj})
        if obj_el is None:  # there was not an entry with _id equal to key in the dict!
            # ignore the parent !
            obj_el = {'_id': obj, 'parent': obj, 'weight': 1}
        else:  # there is already an entry with _id equal to key!
            parent_el = self.db[self.collection].find_one({'_id': parent})
            obj_el['parent'] = parent_el['_id']
        self.db[self.collection].save(obj_el)

    def inc_weight(self, obj, weight):
        obj_el = self.db[self.collection].find_one({'_id': obj})
        obj_el['weight'] += weight
        self.db[self.collection].save(obj_el)

    def items(self):
        res = {el.pop('_id'): el for el in self.db[self.collection].find()}
        for el in res.items():
            yield el


class DictParents(Parents):
    """
    Handle disjoint sets, using built-in python dictionaries
    """
    def __init__(self):
        self._parents = {}

    def __contains__(self, obj):
        return obj in self._parents

    def __getitem__(self, obj):
        return self._parents[obj]

    def __setitem__(self, obj, parent):
        if obj not in self._parents:
            self._parents[obj] = {'parent': parent, 'weight': 1}
        else:
            self._parents[obj]['parent'] = parent

    def inc_weight(self, obj, weight):
        self._parents[obj]['weight'] += weight

    def items(self):
        for el in self._parents.items():
            yield el

    def consolidate(self, db, collection):
        if isinstance(db, pymongo.database.Database):
            MongoConsolidate(db, collection).consolidate(self._parents)
        elif isinstance(db, MySQLdb.connections.Connection):
            MySQLConsolidate(db, collection).consolidate(self._parents)
        else:
            raise TypeError('db must be an instance of pymongo.database.Database or MySQLdb.connections.Connection')


class Consolidate(object):
    """
    Abstract class that provides methods to consolidate in-memory python dictionaries
    to databases. Sub-classes must implement per-database consolidation methods
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, db):
        """ Initialize the class with an instance of db and the collection where to write to """
        self.db = db

    @abc.abstractmethod
    def consolidate(self, dict_to_consolidate):
        """ Write the contents of the argument in a database table/collection """
        return


class MongoConsolidate(Consolidate):
    def __init__(self, db, collection):
        """
        Consolidate in-memory disjoint sets in a mongodb collection

        Parameters
        -----------
        :param db: Instance of pymongo.database.Database. Results will be stored here.
        :param collection: String specifying the collection where to store the results. Collection is emptied if it already exists.
        """
        if not isinstance(db, pymongo.database.Database):
            raise TypeError('db must be a valid instance of pymongo.database.Database')
        self.collection = collection
        super(MongoConsolidate, self).__init__(db)

    def consolidate(self, dict_to_consolidate):
        self.db.drop_collection(self.collection)
        return self.db[self.collection].insert([dict(v, **{"_id": k}) for k, v in dict_to_consolidate.items()])


class MySQLConsolidate(Consolidate):
    def __init__(self, db, table):
        if not isinstance(db, MySQLdb.connections.Connection):
            raise TypeError('db must be a valid instance of MySQLdb.connections.Connection')
        self.cur = db.cursor(MySQLdb.cursors.DictCursor)
        self.table = table
        super(MySQLConsolidate, self).__init__(db)

    def consolidate(self, dict_to_consolidate):
        values = []
        # unfold the dictionary
        for k, v in dict_to_consolidate.items():  # this will speed-up database insertions
            values.append((k, v['parent'], v['weight']))

        with self.db:
            self.cur.execute('DROP TABLE IF EXISTS %s' % self.table)
            self.cur.execute('CREATE TABLE %s (_id varchar(100) NOT NULL PRIMARY KEY,'
                             ' parent varchar(100), weight int)'
                             'DEFAULT CHARACTER SET utf8 COLLATE utf8_bin'  # necessary to allow unicode comparisons
                             % self.table)
            query = "INSERT INTO `%s` " % self.table
            query += "VALUES (%s, %s, %s)"
            self.cur.executemany(query, values)

        return dict_to_consolidate.keys()


class UnionFind:
    """Union-find data structure.

    Each unionFind instance X maintains a family of disjoint sets of
    hashable objects, supporting the following two methods:

    - X[item] returns a name for the set containing the given item.
      Each set is named by an arbitrarily-chosen one of its members; as
      long as the set remains unchanged it will keep the same name. If
      the item is not yet part of a set in X, a new singleton set is
      created for it.

    - X.union(item1, item2, ...) merges the sets containing each item
      into a single larger set.  If any item is not yet part of a set
      in X, it is added to X as one of the members of the merged set.

    """
    def __init__(self, db=None, collection=None, storage='mongodb'):
        """Create a new empty union-find structure."""
        if db is None or collection is None or storage not in available_storage:
            self.parents = DictParents()
        elif storage == 'mongodb':
            self.parents = MongoParents(db, collection)
        else:  # storage == 'mysql':
            self.parents = MySQLParents(db, collection)


    def __getitem__(self, obj):
        """Find and return the name of the set containing the object."""

        # check for previously unknown object
        if obj not in self.parents:
            self.parents[obj] = obj
            return obj

        # find path of objects leading to the root
        path = [obj]
        root = self.parents[obj]['parent']
        while root != path[-1]:
            path.append(root)
            root = self.parents[root]['parent']

        # compress the path and return
        for ancestor in path:
            self.parents[ancestor] = root
        return root

    def union(self, *objects):
        """Find the sets containing the objects and merge them all."""
        roots = [self[x] for x in objects]
        heaviest = max([(self.parents[r]['weight'], r) for r in roots])[1]
        for r in roots:
            if r != heaviest:
                self.parents.inc_weight(heaviest, self.parents[r]['weight'])
                self.parents[r] = heaviest

    def deunion(self, *objects):
        """Remove each object from the set it currently belongs to and put it into a singleton"""
        # to avoid breaking currently existing paths, we must be sure all paths are compressed
        # indeed, self.items() actually compress paths before returning items
        items = [item for item in self.items()]
        for obj in objects:
            # pick only the guys that have obj as their parent, i.e.:
            # -- all the guys in set containing obj, or,
            # -- none if obj have been elected as the parent of the set that contains it
            cur_set = [item[0] for item in items if item[1] == obj and item[0] != obj]
            for el in cur_set:
                self.parents[el] = cur_set[0]  # el[0] arbitrarily becomes the new set representative, i.e. the parent
            self.parents[obj] = obj  # and obj ends up in a singleton containing itself, only.


    def consolidate(self, db, collection):
        return self.parents.consolidate(db, collection)

    def items(self):
        """
        Return 2-tuples containing element and root of the set containing it, compressing each existing path.
        """
        for item in self.parents.items():
            yield (item[0], self[item[0]])
