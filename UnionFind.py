import pymongo
"""UnionFind.py

Original Union-find data structure based on Josiah Carlson's code,
http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/215912
with significant additional changes by D. Eppstein.

Changes to include mongodb persistence for disjoint sets included by Simone Mainardi
"""

class Parents:
    def __init__(self, db=None, collection=None):
        if not isinstance(db, pymongo.database.Database) and not db == None:
            raise TypeError('db is neither a valid instance of pymongo.database.Database nor None')

        if db is None:
            self._parents = {}
        
        self.db = db
        self.collection = collection
            
    def __contains__(self, obj):
        if self.db is not None:
            return obj in list(self.db[self.collection].find({'_id': obj}, {'_id': 1}))
        else:
            return obj in self._parents
            
    def __getitem__(self, obj):
        if self.db is not None:
            return self.db[self.collection].find_one({'_id': obj})
        else:
            return self._parents[obj]

    def __setitem__(self, obj, parent):
        if self.db is not None:
            obj_el = self.db[self.collection].find_one({'_id': obj})
            if obj_el is None:  # there was not an entry with _id equal to key in the dict!
                # ignore the parent !
                obj_el = {'_id': obj, 'parent': obj, 'weight': 1}
            else:  # there is already an entry with _id equal to key!
                parent_el = self.db[self.collection].find_one({'_id': parent})
                obj_el['parent'] = parent_el['_id']
            self.db[self.collection].save(obj_el)                
        else:
            if obj not in self._parents:
                self._parents[obj] = {'parent': parent, 'weight': 1}
            else:
                self._parents[obj]['parent'] = parent

    def inc_weight(self, obj, weight):
        if self.db is not None:
            obj_el = self.db[self.collection].find_one({'_id': obj})
            obj_el['weight'] += weight
            self.db[self.collection].save(obj_el)
        else:
            self._parents[obj]['weight'] += weight

    def items(self):
        if self.db is not None:
            res = {el.pop('_id'): el for el in self.db[self.collection].find()}
        else:
            res = self._parents
        for el in res.items():
            yield el

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
    def __init__(self, db=None, collection=None):
        """Create a new empty union-find structure."""
        self.parents = Parents(db, collection)

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
        roots = [self.parents[x]['parent'] for x in objects]
        heaviest = max([(self.parents[r]['weight'], r) for r in roots])[1]
        for r in roots:
            if r != heaviest:
                self.parents.inc_weight(heaviest, self.parents[r]['weight'])
                self.parents[r] = heaviest

    def consolidate(self, db, collection):
        """
        Consolidate in-memory disjoint sets in a mongodb collection

        Parameters
        -----------
        :param db: Instance of pymongo.database.Database. Results will be stored here.
        :param collection: String specifying the collection where to store the results. Collection is emptied if it already exists.
        """
        db.drop_collection(collection)
        return db[collection].insert([dict(v, **{"_id": k}) for k, v in self.parents.items()])
