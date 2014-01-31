
from dfilter import Dfilter


class TestFunctional(object):

    def setup(self):
        data = {'a': 1, 'b': 2}
        data['foo'] = {'name': 'foo', 'friend': ['bar', 'qux'], 'age': 10}
        data['bar'] = {'name': 'bar', 'friend': ['foo'], 'age': 70}
        data['qux'] = {'name': 'qux', 'friend': ['bar'], 'age': 40}
        self.df = Dfilter(data)

    def test_find(self):
        query = {'*.name': 'foo'}
        items = self.df.find(query)
        print items
        assert 'foo' in items.keys(), 'Check that foo is returned'
        query = {'*.name': {'$in': ['foo', 'bar']}}
        items = self.df.find(query)
        assert 'foo' in items.keys(), 'Check that foo is returned'
        assert 'bar' in items.keys(), 'Check that bar is returned'
        query = {'*.name': {'$contains': 'oo'}}
        items = self.df.find(query)
        assert 'foo' in items.keys(), 'Check that foo is returned'
        query = {'*.age': {'$lt': 50}}
        items = self.df.find(query)
        assert 'foo' in items.keys(), 'Check that foo is returned'
        assert 'qux' in items.keys(), 'Check that qux is returned'
        assert 'bar' not in items.keys(), 'Check that bar is not returned'
        query = {'bar.age': {'$gt': 5}}
        items = self.df.find(query)
        assert 'foo' not in items.keys(), 'Check that foo is returned'
        assert 'qux' not in items.keys(), 'Check that qux is returned'
        assert 'bar' in items.keys(), 'Check that bar is not returned'

    def test_fetch(self):
        friend = self.df.fetch('foo.friend.1')
        assert friend == 'qux', "Check that correct item is given back. Got {}".format(friend)
        friend = self.df.fetch('foo.friend.10', default='Dontknow')
        assert friend == 'Dontknow', "Check that correct item is given back. Got {}".format(friend)

    def test_flattend(self):
        items = self.df.flatten()
        assert items.get('qux.name') == 'qux', "Check that data was flattened"

    def test_fields(self):
        items = self.df.fields('qux.name')
        qux = items.get('qux')
        assert items.get('qux'), "Check that the data is returned"
        assert qux == {'name': 'qux'}, \
            "Check that the data is returned {}".format(qux)
        assert items.get('qux').get('age') is None, "Check that age is not there"
        items = self.df.fields('*.name')
        assert items.get('qux'), "Check that the data is returned"
        assert items.get('bar'), "Check that the data is returned"
        assert items.get('qux').get('age') is None, "Check that age is not there"
        items = self.df.fields(['qux.name', 'bar.name'])
        assert items.get('qux'), "Check that the data is returned"
        assert items.get('bar'), "Check that the data is returned"
        assert items.get('qux').get('age') is None, "Check that age is not there"

    def test_spot_unpack(self):
        test_dict = {'a': 1, 'b': 2}
        path = self.df._unpack_step('*', test_dict)
        assert path == test_dict.keys(), "Check that all keys are in the result"
        path = self.df._unpack_step('a', test_dict)
        assert path == ['a'], "Check that a is returned."
        path = self.df._unpack_step('x', test_dict)
        assert not path and isinstance(path, list), "Make sure we get an empty list."
        path = self.df._unpack_step('*', list('abcdefghji'))
        assert path == range(10), ''
        path = self.df._unpack_step('3', list('abcdefghji'))
        assert path == [3], ''
        path = self.df._unpack_step(3, list('abcdefghji'))
        assert path == [3], ''
        path = self.df._unpack_step('[3,4]', list('abcdefghji'))
        assert path == [3, 4], ''
        path = self.df._unpack_step('[3, 4]', list('abcdefghji'))
        assert path == [3, 4], ''
        path = self.df._unpack_step('[ 3 ,  4 , 5  ]', list('abcdefghji'))
        assert path == [3, 4, 5], ''
        path = self.df._unpack_step('[*, 4]', list('abcdefghji'))
        assert path == range(10), '{0}'.format(path)



#TestDataSample1
tds1 = {"menu": {"header": "SVG Viewer",
                 "items": [{"id": "Open"},
                           {"id": "OpenNew", "label": "Open New"},
                           {"id": "ZoomIn", "label": "Zoom In"},
                           {"id": "ZoomOut", "label": "Zoom Out"},
                           {"id": "OriginalView", "label": "Original View"},
                           {"id": "Quality"},
                           {"id": "Pause"},
                           {"id": "Mute"},
                           None,
                           {"id": "Find", "label": "Find..."},
                           {"id": "FindAgain", "label": "Find Again"},
                           {"id": "Copy"},
                           {"id": "CopyAgain", "label": "Copy Again"},
                           {"id": "CopySVG", "label": "Copy SVG"},
                           {"id": "ViewSVG", "label": "View SVG"},
                           {"id": "ViewSource", "label": "View Source"},
                           {"id": "SaveAs", "label": "Save As"},
                           None,
                           {"id": "Help"},
                           {"id": "About", "label": "About Adobe CVG Viewer"}
                           ],
                 'hidden': [{"id": "FindAgain", "label": "Find Again"},
                            {"id": "CopyAgain", "label": "Copy Again"}
                            ]
                 }
        }
