from UnionFind import UnionFind


def func(x):
    return x + 2


def test_answer():
    assert func(3) == 5


class TestUnionFind:
    uf = None
    guys = ['nathan', 'mike', 'john', 'albert']

    def setup_class(self):
        self.uf = UnionFind()
        assert self.uf is not None

    def test_insertion(self):
        for guy in self.guys:
            res = self.uf[guy]
            assert res == guy
        for guy in self.guys:
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

        # weights
        assert self.uf.parents['mike']['weight'] == self.uf.parents['mike']['weight']

    def test_contents(self):
        assert 2 == 2


