#!/usr/bin/env python

import json
import operator
from collections import OrderedDict
import dpath.util


class Dfilter(object):

    def __init__(self, data=None, separator='.'):
        self.separator_char = str(separator)[0]
        self.data = OrderedDict()
        self._store(data)

    def _store(self, data):
        if not data:
            pass
        elif isinstance(data, dict):
            self.data.update(data)
        elif isinstance(data, list):
            self.data.update(dict([(str(n[0]), n[1])
                                   for n in enumerate(data)]))

    def read_json(self, fh):
        close_file = False
        if isinstance(fh, str):
            fh = open(fh, 'r')
        self._store(json.loads(fh.read()))
        if close_file:
            fh.close()

    def select(self, query):
        if isinstance(query, list):
            query = str(self.separator_char).join(query)
        data = dpath.util.search(self.data, query,
                                 separator=self.separator_char)
        return Dfilter(data)

    def iselect(self, query):
        return dpath.util.search(self.data, query, yielded=True)

    def fields(self, fields):
        if isinstance(fields, str):
            if ',' in fields:
                fields = [i.strip() for i in fields.split(",")]
            else:
                fields = [fields]

        return Dfilter(dict([(k, self.data[k])
                             for k in self.data
                             if k in fields]))

    def flatten(self, depth=1):
        return Dfilter(self._flatten(self.data, depth))

    def _flatten(self, indata, depth=1):
        data = {}
        for key1 in indata:
            if isinstance(self.data[key1], dict):
                for key2 in indata[key1]:
                    key_name = "{0}{1}{2}".format(key1, self.separator_char, key2)
                    data[key_name] = indata[key1][key2]
            else:
                data[key1] = indata[key1]
        return data

    def fold(self):
        """A one dimentional Dictionary.
        All the values are keys with each a list that contains the path.
        :return: Dfilter Object.
        """
        #TODO(Martin): Make this multidimentional.
        return Dfilter(dict([(str(self.data[k], [k])) for k in self.data]))

    def _filter_func(self, name):
        name = name.strip('$')
        if hasattr(operator, name):
            return getattr(operator, name)
        elif name == 'in':
            return lambda a, b: a in b
        elif name == 'nin':
            return lambda a, b: a not in b
        elif name == 'all':
            return lambda a, b: all([a in bb for bb in b])
        elif name == 'mod':
            return lambda a, b: a % b[0] == b[1]
        elif name == 'exists':
            return lambda a, b: (b and a is not None) or (a is None and not b)
        else:
            # Hope nobody ever gets here.
            return lambda a, b: False

    def _evaluate(self, data, path, oper, comp):
        print 'D', data, path, oper, comp
        for item in dpath.util.search(data, path, yielded=True):
            print "I", item
            if self._filter_func(oper)(item[1], comp):
                return True
        return False

    def find(self, query):
        """Query is a filter dict like in mongodb."""
        tests = []
        for key in query:
            print key
            value = query[key]
            if isinstance(value, str) or isinstance(value, unicode):
                op = '$eq'
                val = value
            else:
                op = [n for n in value if n.startswith('$')]
                if len(op) > 0:
                    op = op[0]
                    val = value[op]
                else:
                    print "NOP", value, op
            if key and op:
                tests.append((key, op, val))

        print(tests)
        selected_items = []
        for item in self.data:
            if all([self._evaluate(self.data[item], test[0], test[1], test[2])
                    for test in tests]):
                selected_items.append(item)
        return Dfilter(dict([(k, self.data[k]) for k in selected_items]))

    def count(self):
        count = 0
        for key in self.data:
            count += 1
        return count

    def __repr__(self):
        return self.data

    def __str__(self):
        return json.dumps(self.data, indent=4)
        #return str(self.data)

    def values(self, sort=True):
        values = list()
        for key in self.data:
            values.append(self.data[key])
        if sort:
            return sorted(values)
        else:
            return values

    def limit(self, number=100):
        return self.slice(0, number)

    def first(self, number=1):
        return self.slice(0, number)

    def last(self, number=1):
        return self.slice(-1, -1 - abs(number))

    def slice(self, start=0, end=-1):
        start, end = int(start), int(end)
        if start > end:
            raise ValueError("start should not be bigger than end")

        items = self.data.keys()[start:end]
        return Dfilter([(k, self.data) for k in items])

    def uniqe_values(self, sort=True):
        values = set()
        for key in self.data:
            values.add(self.data[key])
        if sort:
            return sorted(list(values))
        else:
            return list(values)

    def sort(self, fields=None, func=None):
        #TODO(Martin): Order based on fields not keys.
        return Dfilter(OrderedDict([(k, self.data[k])
                                    for k in sorted(self.data)]))

    def __getattr__(self, name):
        if hasattr(self.data, name):
            return getattr(self.data, name)

### Sugestions:
# Unwind: Dict with lists, become lists with dicts.
# Group: add a extra layer to dict where similar dicts are in the dict...
# skip:
# limit:


if __name__ == "__main__":
    df = Dfilter(separator='#')
    df.read_json("tests/world_bank_countries.json")
    print df.select('*#name').flatten().uniqe_values()
    print df
    print df.count()

