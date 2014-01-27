

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
        print items
        assert 'foo' in items.keys(), 'Check that foo is returned'
        query = {'name': {'$in': ['foo', 'bar']}}
        items = self.df.find(query)
        assert 'foo' in items.keys(), 'Check that foo is returned'
        assert 'bar' in items.keys(), 'Check that bar is returned'
        query = {'name': {'$contains': 'oo'}}
        items = self.df.find(query)
        assert 'foo' in items.keys(), 'Check that foo is returned'
        query = {'age': {'$lt': 50}}
        items = self.df.find(query)
        assert 'foo' in items.keys(), 'Check that foo is returned'
        assert 'qux' in items.keys(), 'Check that qux is returned'
        assert 'bar' not in items.keys(), 'Check that bar is not returned'

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
