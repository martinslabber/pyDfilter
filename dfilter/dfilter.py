#!/usr/bin/env python

import json
import operator

try:
    from collections import OrderedDict as odict
except ImportError:
    # Python 2.6 has no ordered dict, we use a normal dict.
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

    def __getitem__(self, key):
        return self.data[key]

    def __add__(self, other):
        #raise Value Error if we do not add.
        if isinstance(self.data, list):
            # Check that other is also a list.
            return self.data + other
        else:
            return {'implimented': 'Not'}

    def __len__(self):
        return self.data.__len__()

    def clean(self, data=None):
        if data is None:
            data = self.data

        self.data = json.loads(json.dumps(data))
        return self

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
            self.data = data
            #self.data.update(dict([(str(n[0]), n[1])
            #                       for n in enumerate(data)]))

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

        #if isinstance(self.data, dict):
        new_data = {}
        for field in fields:
            for item in self.spot(field):
                key_data = new_data
                for key in item[0][:-1]:
                    if key not in key_data:
                        key_data[key] = {}
                    key_data = key_data[key]
                key_data[item[0][-1]] = item[1]

        if isinstance(self.data, list):
            return Dfilter([new_data[n] for n in new_data])
        #    new_data = []
        #    for field in fields:
        #        for item in self.spot(field):
        #            #key_data = new_data
        #            #for key in item[0][:-1]:
        #            #    if key not in key_data:
        #            #        key_data[key] = {}
        #            #    key_data = key_data[key]
        #            #key_data[item[0][-1]] = item[1]
        #            new_data.append(item[1])
        #else:
        #    return None
        else:
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
            if type(items[0]) in [list, dict]:
                return Dfilter(items[0])
            else:
                return items[0]
        else:
            return Dfilter(items)

    def filter(self, query):
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

        selected_items = []
        if isinstance(self.data, list):
            sequence = range(len(self.data))
        else:
            sequence = self.data.keys()
        for item in sequence:
            if all([self._evaluate({item: self.data[item]},
                                   test[0], test[1], test[2])
                    for test in tests]):
                selected_items.append(item)
        if isinstance(self.data, list):
            return Dfilter([self.data[k] for k in selected_items])
        else:
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

    def values(self, sort=False):
        """Return a list of values.

        Like dictionary get().

        Params
        ------
            sort: Boolean

        Return
        ------
            List of values.

        >>> Dfilter([1, 2, 3]).values()
        [1, 2, 3]
        >>> Dfilter([1, 3, 2]).values(True)
        [1, 2, 3]
        >>> Dfilter({'magician': 'david', 'assistant': 'chris'}).values(True)
        ['chris', 'david']

        """
        values = list()
        if isinstance(self.data, dict):
            values = self.data.values()
        else:
            values = self.data

        if sort:
            return sorted(values)
        else:
            return values

    def unique_values(self, sort=True):
        """Return a list of unique values.

        Like dictionary get() but only unique values are returned.

        Params
        ------
            sort: Boolean, default to True.

        Return
        ------
            List of values.

        >>> Dfilter([1, 3, 2, 3]).unique_values()
        [1, 2, 3]
        >>> Dfilter([2, 1, 3, 2]).unique_values(True)
        [1, 2, 3]
        >>> Dfilter({'m': 'dan', 'a': 'phil', 'o': 'dan'}).unique_values(True)
        ['dan', 'phil']

        """
        values = set(self.values())
        if sort:
            return sorted(list(values))
        else:
            return list(values)

    def limit(self, number=100):
        """Return the up to the number of items.

        >>> Dfilter(range(9)).limit(5)
        [0, 1, 2, 3, 4]

        """
        return self.slice(0, number)

    def first(self, number=1):
        """Return the first item or up to the number of items.

        Alias for limit but default is to return 1 item.

        >>> Dfilter(range(9)).first()
        [0]

        """
        return self.slice(0, number)

    def last(self, number=1):
        """Return the last item or up to the number of items from the back.

        >>> Dfilter(range(9)).last()
        [8]
        >>> Dfilter(range(9)).last(2)
        [7, 8]

        """
        return self.slice(start=-1 * number, end=0)

    def slice(self, start=None, end=None):
        """

        TODO: Support slicing dictionaries on keys.

        Params
        ------
            start: integer

            end: integer

        >>> Dfilter([1, 2, 3]).slice(0, 1)
        [1]

        """
        if start is None:
            start = 0
        start = int(start)
        if end is None:
            end = 0
        end = int(end)

        if start > end:
            pass
            #raise ValueError("start can not be bigger than end")

        if isinstance(self.data, list):
            if end:
                items = self.data[start:end]
            else:
                items = self.data[start:]
        else:
            if end == 0:
                items = dict(self.data.items()[start])
            else:
                items = dict(self.data.items()[start:end])

        return Dfilter(items)

    def sort(self, fields=None, func=None):
        #TODO(Martin): Order based on fields not keys.
        return Dfilter(odict([(k, self.data[k])
                              for k in sorted(self.data)]))

    def get(self, key, default=None):
        """Return the value for the given key or the default value.

        Like dictionary get().

        Params
        ------
            key:
                Return the value represented by this key.
            default:
                If key is not found return this as the value.

        Return
        ------
            Value

        >>> Dfilter(['d', 'a', 'y']).get(1)
        'a'
        >>> Dfilter([1, 2, 3]).get(10, 'Out of stock!')
        'Out of stock!'
        >>> Dfilter({'name': 'Jim', 'age': 4}).get('name', 'Bob')
        'Jim'
        >>> Dfilter({'name': 'Jim', 'age': 4}).get(1)

        >>> Dfilter({'name': 'Jim'}).get('age', 0)
        0

        """
        if isinstance(self.data, dict):
            return self.data.get(key, default)
        else:
            try:
                return self.data[key]
            except KeyError:
                return default
            except IndexError:
                return default

    def keys(self):
        """List of keys. df.keys() -> list of df's keys.

        Like dictionary key().

        Return
        ------
            List of Keys

        >>> Dfilter(['d', 'a', 'y']).keys()
        [0, 1, 2]
        >>> Dfilter([]).keys()
        []
        >>> sorted(Dfilter({'name': 'Jim', 'age': 4}).keys())
        ['age', 'name']
        >>> Dfilter().keys()
        []

        """
        if isinstance(self.data, dict):
            return self.data.keys()
        else:
            return range(len(self.data))

    def items(self):
        """Return the (key, value) pairs.

        Like dictionary items().

        Return
        ------
            Iterator of (key, value) pairs as 2-tuples

        >>> list(Dfilter(['d', 'a', 'y']).items())
        [(0, 'd'), (1, 'a'), (2, 'y')]
        >>> list(Dfilter({'day': 2, 'month': 6}).items())
        [('day', 2), ('month', 6)]

        """
        if isinstance(self.data, dict):
            return self.data.items()
        else:
            return enumerate(self.data)

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
#
