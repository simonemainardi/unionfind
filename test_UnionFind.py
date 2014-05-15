__author__ = 'simone'
import unittest
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


class UnionFindTestCase(unittest.TestCase):
    def setUp(self):
        self.uf = UnionFind()

    def test_insertion(self):
        guys = ['nathan', 'mike', 'john', 'albert']
        for guy in guys:
            res = self.uf[guy]
            assert res == guy
        for guy in guys:
            assert guy in self.uf.parents

    def test_union(self):
        self.test_insertion()
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
        self.test_union()
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

    def test_iter_sets(self):
        self.test_deunion()
        self.uf.deunion('albert', 'john', 'mike', 'nathan')
        roots = ['albert', 'john', 'mike', 'nathan']
        res = []
        try:
            self.uf.iter_sets()
        except NotImplementedError:  # implementation may not exists
            return
        for el in self.uf.iter_sets():
            assert len(el) == 1
            res.append(el[0])
        assert(sorted(roots) == sorted(res))
        self.uf.union('albert', 'john', 'mike')
        for el in self.uf.iter_sets():
            assert len(el) == 1 or len(el) == 3
            if len(el) == 3:
                assert set(['albert', 'john', 'mike']) == set(el)
            else:
                assert set(['nathan']) == set(el)


class MongoUnionFindTestCase(UnionFindTestCase):
    def setUp(self):
        mongo_client.drop_database(mongo_db)
        self.uf = UnionFind(mongo_db, mongo_collection, 'mongodb')

    def tearDown(self):
        mongo_client.drop_database(mongo_db)


class MySQLUnionFindTestCase(UnionFindTestCase):
    def setUp(self):
        with mysql_db:
            cur = mysql_db.cursor(MySQLdb.cursors.DictCursor)
            cur.execute('DROP TABLE IF EXISTS %s' % mysql_table)
            cur.execute('CREATE TABLE %s (_id varchar(100) NOT NULL PRIMARY KEY,'
                        'parent varchar(100), weight int)'
                        'DEFAULT CHARACTER SET utf8 COLLATE utf8_bin' % mysql_table)
        self.uf = UnionFind(mysql_db, mysql_table, 'mysql')

    def tearDown(self):
        with mysql_db:
            cur = mysql_db.cursor(MySQLdb.cursors.DictCursor)
            cur.execute('DROP TABLE IF EXISTS %s' % mysql_table)


class MongoConsolidateUnionFindTestCase(UnionFindTestCase):
    def test_consolidate_mongodb(self):
        self.test_deunion()
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


class MySQLConsolidateUnionFindTestCase(UnionFindTestCase):
    def setUp(self):
        with mysql_db:
            cur = mysql_db.cursor(MySQLdb.cursors.DictCursor)
            cur.execute('DROP TABLE IF EXISTS %s' % mysql_table)
        self.uf = UnionFind()

    def test_consolidate_mysql(self):
        self.test_deunion()
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


    def test_consolidate_mysql_extra_fields(self):
        self.uf.deunion()
        self.uf.consolidate(mysql_db, mysql_table, gender='male', country='USA', state='NY')
        # instantiate a new unionfind instance that uses db stuff
        uf2 = UnionFind(mysql_db, mysql_table, 'mysql', gender='male', country='USA', state='NY')

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


class ExtraFieldsTestCase(unittest.TestCase):
    def setUp(self):
        with mysql_db:
            cur = mysql_db.cursor(MySQLdb.cursors.DictCursor)
            cur.execute('DROP TABLE IF EXISTS %s' % mysql_table)

    def _select_star(self):
        cur = mysql_db.cursor(MySQLdb.cursors.DictCursor)
        cur.execute('SELECT * FROM %s' % mysql_table)
        return cur.fetchall()

    def test(self):
        uf = UnionFind()
        uf.union('alpha', 'bravo', 'charlie', 'delta')
        uf.consolidate(mysql_db, mysql_table, role='player', type='individual')
        for el in self._select_star():
            self.assertSetEqual(set(['role', 'type', '_id', 'parent', 'weight']), set(el.keys()))
            self.assertEqual(el['role'], 'player')
            self.assertEqual(el['type'], 'individual')

        uf = UnionFind(mysql_db, mysql_table, 'mysql',  role='player', type='organization')
        uf.union('adams', 'boston', 'chicago')
        for el in self._select_star():
            if el['type'] == 'organization':
                self.assertIn(el['_id'], ['adams', 'boston', 'chicago'])
            elif el['type'] == 'individual':
                self.assertIn(el['_id'], ['alpha', 'bravo', 'charlie', 'delta'])
            else:
                raise self.failureException


if __name__ == '__main__':
    unittest.main()
