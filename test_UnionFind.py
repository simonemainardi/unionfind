from UnionFind import UnionFind
from pymongo import MongoClient
import MySQLdb

dbhost = 'localhost'
testdbname = 'test_unionfind'

mongo_client = MongoClient(dbhost)
mongo_db = mongo_client[testdbname]
mongo_collection = 'unionfind'

mysql_table = 'unionfind'
mysql_db = MySQLdb.connect(dbhost, 'test', '', testdbname)

class TestUnionFind:
    def __init__(self, _db=None, _collection=None, _storage='mongodb'):
        self.uf = UnionFind(_db, _collection, _storage)
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

        it_dict = {k: v for k, v in self.uf.items()}
        assert 'john' in it_dict and it_dict['john'] == 'john'
        assert 'albert' in it_dict and it_dict['albert'] == 'john'
        assert 'nathan' in it_dict and it_dict['nathan'] == 'nathan'
        assert 'mike' in it_dict and it_dict['mike'] == 'nathan'
        it_dict.clear()

        # weights
        assert self.uf.parents['nathan']['weight'] == self.uf.parents['mike']['weight'] + 1 == 2
        assert self.uf.parents['john']['weight'] == self.uf.parents['albert']['weight'] + 1 == 2

        # one more union !
        self.uf.union('mike', 'albert')
        assert self.uf['mike'] == self.uf['albert']
        self.uf.parents['albert']['weight'] == self.uf.parents['mike']['weight'] + 2 == 4

        # now we shoud have only one set
        it_dict = {k: v for k, v in self.uf.items()}
        assert 'john' in it_dict and it_dict['john'] == 'nathan'
        assert 'albert' in it_dict and it_dict['albert'] == 'nathan'
        assert 'nathan' in it_dict and it_dict['nathan'] == 'nathan'
        assert 'mike' in it_dict and it_dict['mike'] == 'nathan'
        it_dict.clear()

    def test_deunion(self):
        self.uf.deunion('nathan')
        assert self.uf['nathan'] == self.uf['nathan']  # nathan was the parent of the set
        assert self.uf['john'] != self.uf['nathan']
        assert self.uf['mike'] != self.uf['nathan']
        assert self.uf['albert'] != self.uf['nathan']
        assert self.uf['john'] == self.uf['mike'] == self.uf['albert']  # those guys are still in the same set

        # add nathan again
        self.uf.union('nathan', 'mike')
        # and try a deunion with multiple items
        self.uf.deunion('albert', 'john')
        assert self.uf['albert'] == self.uf['albert']
        assert self.uf['john'] == self.uf['john']
        assert self.uf['mike'] == self.uf['nathan'] != self.uf['albert'] != self.uf['john']

        # de union everyone
        self.uf.deunion('albert', 'john', 'mike', 'nathan')
        assert self.uf['mike'] == self.uf['mike']
        assert self.uf['john'] == self.uf['john']
        assert self.uf['nathan'] == self.uf['nathan']
        assert self.uf['albert'] == self.uf['albert']


    def test_consolidate(self):
        mongo_db.drop_collection(mongo_collection)
        self.uf.consolidate(mongo_db, mongo_collection)
        # instantiate a new unionfind instance that uses db stuff
        uf2 = UnionFind(mongo_db, mongo_collection)

        for el in mongo_db[mongo_collection].find():
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

    def test_consolidate_mongodb(self):
        mongo_db.drop_collection(mongo_collection)
        self.uf.consolidate(mongo_db, mongo_collection)
        # instantiate a new unionfind instance that uses db stuff
        uf2 = UnionFind(mongo_db, mongo_collection)

        for el in mongo_db[mongo_collection].find():
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

    def test_consolidate_mysql(self):
        self.uf.consolidate(mysql_db, mysql_table)
        # instantiate a new unionfind instance that uses db stuff
        uf2 = UnionFind(mysql_db, mysql_table, 'mysql')

        cur = mysql_db.cursor(MySQLdb.cursors.DictCursor)
        cur.execute('SELECT * FROM %s' % mysql_table)
        for el in cur.fetchall():
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
    tuf.test_deunion()


def test_unionfind_mongodb():
    mongo_client.drop_database(mongo_db)

    tuf = TestUnionFind(mongo_db, mongo_collection)
    tuf.test_insertion()
    tuf.test_union()
    tuf.test_deunion()

    mongo_client.drop_database(mongo_db)


def test_unionfind_mysql():
    with mysql_db:
        cur = mysql_db.cursor(MySQLdb.cursors.DictCursor)
        cur.execute('DROP TABLE IF EXISTS %s' % mysql_table)
        cur.execute('CREATE TABLE %s (_id varchar(100) NOT NULL PRIMARY KEY,'
                    'parent varchar(100), weight int)'
                    'DEFAULT CHARACTER SET utf8 COLLATE utf8_bin' % mysql_table)

    tuf = TestUnionFind(mysql_db, mysql_table, 'mysql')
    tuf.test_insertion()
    tuf.test_union()
    tuf.test_deunion()

    with mysql_db:
        cur = mysql_db.cursor(MySQLdb.cursors.DictCursor)
        cur.execute('DROP TABLE IF EXISTS %s' % mysql_table)

def test_unionfind_consolidate_mongodb():
    mongo_client.drop_database(mongo_db)

    tuf = TestUnionFind()
    tuf.test_insertion()
    tuf.test_union()
    tuf.test_deunion()
    tuf.test_consolidate_mongodb()

    mongo_client.drop_database(mongo_db)

def test_unionfind_consolidate_mysql():
    with mysql_db:
        cur = mysql_db.cursor(MySQLdb.cursors.DictCursor)
        cur.execute('DROP TABLE IF EXISTS %s' % mysql_table)

    tuf = TestUnionFind()
    tuf.test_insertion()
    tuf.test_union()
    tuf.test_deunion()
    tuf.test_consolidate_mysql()
