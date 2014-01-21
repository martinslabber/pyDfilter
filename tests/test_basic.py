

import os
from dfilter import Dfilter

class TestFunctional(object):

    def setup(self):
        data = {'a': 1, 'b': 2}
        data['foo'] = {'name': 'foo', 'friend': ['bar', 'qux'], 'age': 10}
        data['bar'] = {'name': 'bar', 'friend': ['foo'], 'age': 70}
        data['qux'] = {'name': 'qux', 'friend': ['bar'], 'age': 40}
        self.df = Dfilter(data)

    def test_filter(self):
        query = {'name': 'foo'}
        items = self.df.find(query)
        assert 'foo' in items, 'Check that foo is returned'
        query = {'name': {'$in': ['foo', 'bar']}}
        items = self.df.find(query)
        assert 'foo' in items, 'Check that foo is returned'
        assert 'bar' in items, 'Check that bar is returned'
        query = {'name': {'$contains': 'oo'}}
        items = self.df.find(query)
        assert 'foo' in items, 'Check that foo is returned'
