from UnionFind import UnionFind
from pymongo import MongoClient
from pprint import pprint

dbhost = '192.168.70.26'
testdbname = 'test_unionfind'

client = MongoClient(dbhost)
db = client[testdbname]
collection = 'unionfind'


class TestUnionFind:
    def __init__(self, _db=None, _collection=None):
        self.uf = UnionFind(_db, _collection)
        assert self.uf is not None

    def test_insertion(self):
        guys = ['nathan', 'mike', 'john', 'albert']
        for guy in guys:
            res = self.uf[guy]
            assert res == guy
        for guy in guys:
            assert guy in self.uf.parents

    def test_union(self):
        self.uf.union('nathan', 'mike')
        self.uf.union('john', 'albert')

        # parents ...
        assert self.uf.parents['mike']['parent'] == self.uf.parents['nathan']['parent']
        assert self.uf.parents['john']['parent'] == self.uf.parents['albert']['parent']
        assert self.uf.parents['nathan']['parent'] != self.uf.parents['john']['parent']
        assert self.uf.parents['nathan']['parent'] != self.uf.parents['albert']['parent']
        assert self.uf.parents['john']['parent'] != self.uf.parents['nathan']['parent']
        assert self.uf.parents['john']['parent'] != self.uf.parents['mike']['parent']

        # shorter ;)
        assert self.uf['mike'] == self.uf['nathan']
        assert self.uf['john'] == self.uf['albert']
        assert self.uf['nathan'] != self.uf['john']
        assert self.uf['nathan'] != self.uf['albert']
        assert self.uf['john'] != self.uf['nathan']
        assert self.uf['john'] != self.uf['mike']

        # weights
        assert self.uf.parents['nathan']['weight'] == self.uf.parents['mike']['weight'] + 1 == 2
        assert self.uf.parents['john']['weight'] == self.uf.parents['albert']['weight'] + 1 == 2

        # one more union !
        self.uf.union('mike', 'albert')
        assert self.uf['mike'] == self.uf['albert']
        self.uf.parents['albert']['weight'] == self.uf.parents['mike']['weight'] + 2 == 4

    def test_consolidate(self):
        db.drop_collection(collection)
        self.uf.consolidate(db, collection)
        # instantiate a new unionfind instance that uses db stuff
        uf2 = UnionFind(db, collection)

        for el in db[collection].find():
            # check if everything has been correctly stored into the db
            assert el['_id'] in self.uf.parents
            assert el['parent'] == self.uf.parents[el['_id']]['parent']
            assert el['weight'] == self.uf.parents[el['_id']]['weight']

            # check if the new unionfind structure, initialized from
            # the db contents, actually contains the same elements
            assert el['_id'] in uf2.parents
            # does this guy have the same root?
            assert self.uf[el['_id']] == uf2[el['_id']]
            # and the same weight?
            assert self.uf.parents[el['_id']]['weight'] == uf2.parents[el['_id']]['weight']


def test_unionfind():
    tuf = TestUnionFind()
    tuf.test_insertion()
    tuf.test_union()


def test_unionfind_mongodb():
    client.drop_database(db)

    tuf = TestUnionFind(db, collection)
    tuf.test_insertion()
    tuf.test_union()

    client.drop_database(db)


def test_unionfind_consolidate():
    client.drop_database(db)

    tuf = TestUnionFind()
    tuf.test_insertion()
    tuf.test_union()
    tuf.test_consolidate()

    client.drop_database(db)

