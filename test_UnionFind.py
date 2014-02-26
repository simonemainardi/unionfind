from UnionFind import UnionFind
from pymongo import MongoClient
from pprint import pprint

dbhost = '192.168.70.26'
testdbname = 'test_unionfind'

class TestUnionFind:
    def __init__(self, db=None, collection=None):
        self.uf = UnionFind(db, collection)
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

def test_unionfind():
    tuf = TestUnionFind()
    tuf.test_insertion()
    tuf.test_union()

def test_unionfind_mongodb():
    client = MongoClient(dbhost)
    client.drop_database(testdbname)
    db = client[testdbname]
    tuf = TestUnionFind(db, 'test_collection')
    tuf.test_insertion()
    tuf.test_union()
    pprint(list(db['test_collection'].find()))
    client.drop_database(testdbname)