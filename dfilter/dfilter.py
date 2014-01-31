#!/usr/bin/env python

import json
import operator
try:
    from collections import OrderedDict as odict
except ImportError:
    # Python 2.6 has no ordered dict
    odict = dict

odict = dict


class Dfilter(object):

    """Do arbitrary queries on a data object (aka dictionary)."""

    def __init__(self, data=None, **kwargs):
        self.chars = {'separator': '.',
                      'wildcard': '*',
                      'liststart': '[',
                      'listend': ']',
                      'listsplit': ','}
        self.data = odict()
        self.config(**kwargs)
        self._store(data)

    def __iter__(self):
        return self.data.keys()

    def config(self, **kwargs):
        """Configure the Dfilter object.

        Configuration Options:
            separator - Character to use as separator.
            wildcard  - Character to use as wildcard.
            liststart - Character to indicate start of a list
            listend   - Character ti indicate end of list.
            listsplit - Character to indicate separation of items in list.

        :return: Dictionary with configured values. This can NOT
                 be used as input for config.

        """
        char_names = self.chars.keys()
        chars = [self.chars[c] for c in self.chars]
        for char in char_names:
            if char in kwargs:
                candidate = str(kwargs[char])[0]
                if candidate not in chars:
                    self.chars[char] = candidate
        data_loaded = False
        if self.data:
            data_loaded = True
        return {'chars': self.chars, 'data_loaded': data_loaded}

    def _rflatten(self, data):
        """Recursively walk through dictionary and return a iterator of
        key-path and value.

        :return: Iterator of list and value.

        """
        if isinstance(data, dict):
            items = data.keys()
        elif isinstance(data, list):
            items = range(len(data))
        else:
            items = []
            yield [], data
        for item in items:
            for name, value in self._rflatten(data[item]):
                yield [item] + name, value

    def _iflatten(self, data):
        """Place holder for a future supper fast implementation of flatten."""
        return self._rflatten(data)

    def flatten(self):
        """Flatten data to a 1-d dictionary."""
        out_data = odict()
        for path, value in self._iflatten(self.data):
            out_data[self.chars['separator'
                                ].join([str(p) for p in path])] = value
        return Dfilter(out_data)

    def fold(self, as_str=False):
        """Values becomes the key of a 1-d dictionary and keys are lists.

        Used to lookup values in a dictionary.

        """
        out_data = odict()
        for path, value in self._iflatten(self.data):
            if as_str:
                path_str = self.chars['separator'].join([str(p) for p in path])
                if value in out_data:
                    if isinstance(out_data[value], str):
                        out_data[value] = [out_data[value], path_str]
                    else:
                        out_data[value].append(path_str)
                else:
                    out_data[value] = path_str
            else:
                if value not in out_data:
                    out_data[value] = []
                out_data[value].append(path)
        return Dfilter(out_data)

    def read_json(self, fh):
        """Read a file containing JSON formatted data.

        :param fh: File handle or filename.

        """
        close_file = False
        if isinstance(fh, str):
            fh = open(fh, 'r')
        self._store(json.loads(fh.read()))
        if close_file:
            fh.close()

        return self

    def _store(self, data):
        """Internal method to store the new data."""
        ## I really want to do json.loads(json.dumps(data))
        if data and isinstance(data, dict):
            self.data.update(data)
        elif data and isinstance(data, list):
            self.data.update(dict([(str(n[0]), n[1])
                                   for n in enumerate(data)]))

    def fields(self, fields):
        """Return only the selected fields. Preserving list order.

        eg.: {'a':1, 'b': {'c': ['1', {'d': '3'}, 2]}}
        fields = 'a'
        >> {'a': 1}
        fields = ['a', 'b']
        >> {'a':1, 'b': {'c': ['1', {'d': '3'}, 2]}}
        fields = ['b.c']
        >> {'b': {'c':  ['1', {'d': '3'}, 2]}}
        fields = ['b.c.1.d']  # Not the convertsion of list to dict.
        >> {'b': {'c': {1: {'d': '3'}}}}

        :return: Dfilter object.

        """
        if isinstance(fields, str):
            fields = [fields]

        new_data = {}
        for field in fields:
            for item in self.spot(field):
                key_data = new_data
                for key in item[0][:-1]:
                    if key not in key_data:
                        key_data[key] = {}
                    key_data = key_data[key]
                key_data[item[0][-1]] = item[1]

        return Dfilter(new_data)

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

    def _Ifetch(self, data, path):
        #return dpath.util.search(data, path, yielded=True,
        #                         separator=self.chars['separator'])
        return self.spot(path, data=data)

    def _evaluate(self, data, path, oper, comp):
        for item in self._Ifetch(data, path):
            if self._filter_func(oper)(item[1], comp):
                return True
        return False

    def fetch(self, query, default=None):
        """Smarter get.

        eg.: {'a': 1, 'b': {'c': ['1', {'d': '3'}, 2]}}
        query = 'a'
        >> 1
        query = 'b.c'
        >> ['1', {'d': '3'}, 2]
        query = 'b.c.*.d'
        >> 3

        """
        #return dpath.util.search(self.data, query,
        #                         separator=self.chars['separator'])
        from_spot = self.spot(query)
        if not from_spot:
            return default
        items = list([i[1] for i in self.spot(query)])
        if len(items) == 0:
            return default
        elif len(items) == 1:
            return items[0]
        else:
            return list(items)

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
            if all([self._evaluate({item: self.data[item]},
                                   test[0], test[1], test[2])
                    for test in tests]):
                selected_items.append(item)
        return Dfilter(odict([(k, self.data[k]) for k in selected_items]))

    def count(self):
        count = 0
        for key in self.data:
            count += 1
        return count

    def __repr__(self):
        return self.data.__repr__()

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
        return Dfilter(odict([(k, self.data[k])
                              for k in sorted(self.data)]))

    def __getattr__(self, name):
        # Being a bit more restrictive at the moment.
        # Only support reading from internal DB.
        #if hasattr(self.data, name):
        if name in ['get', 'keys']:
            return getattr(self.data, name)

    def _unpack_step(self, step, obj):
        step = str(step)
        cwildcard = '*'
        clistopen = '['
        clistclose = ']'
        clistseparator = ','
        if not (isinstance(obj, list) or isinstance(obj, dict)):
            return []
        if step == cwildcard:
            if isinstance(obj, dict):
                return obj.keys()
            elif isinstance(obj, list):
                return range(len(obj))
            else:
                return []
        elif step.isdigit():
            if isinstance(obj, list):
                istep = int(step)
                return [int(step)] if istep < len(obj) else []
            else:
                return [step, int(step)]
        elif step.startswith(clistopen) and step.endswith(clistclose):
            out = []
            for s1 in step[1:-1].split(clistseparator):
                for s2 in self._unpack_step(s1.strip(), obj):
                    out.append(s2)
            return list(set(out))
        else:
            if isinstance(obj, dict) and step in obj:
                return [step]
            else:
                return []

    def spot(self, path, data=None):
        if data is None:
            data = self.data
        separator = self.chars['separator']
        path = str(path).split(separator)
        objs = [[[], data]]
        for step in path:
            new_objs = []
            for obj in objs:
                if not obj or len(obj) < 2:
                    continue
                potential_steps = self._unpack_step(step, obj[1])
                for ps in potential_steps:
                    if isinstance(obj[1], dict):
                        if ps not in obj[1]:
                            continue
                        new_objs.append([obj[0] + [ps], obj[1][ps]])
                    elif isinstance(obj[1], list):
                        new_objs.append([obj[0] + [ps], obj[1][ps]])
            objs = new_objs
        for obj in objs:
            yield obj

### Sugestions:
# Unwind: Dict with lists, become lists with dicts.
# Group: add a extra layer to dict where similar dicts are in the dict...
# skip:
# limit:


if __name__ == "__main__":
    df = Dfilter(separator='#')
    df.read_json("tests/world_bank_countries.json")
    print df
    print df.count()

