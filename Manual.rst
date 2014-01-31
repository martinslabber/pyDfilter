


Fetch vs. Fields
----------------

Given the dictionary ::

   dd = {'a': 1, 'b':2, 'c': {'d':3, 'e':['4', '5', {'f': 6, 'g': 7}]}}

When we fetch with a query we get the following results: ::
    
    fetch('c.e')
    >> ['4', '5', {'f': 6, 'g': 7}]

When we Fields with the same query we get. ::

    fields('c.e')
    >> {'c': {'e': ['4', '5', {'f': 6, 'g': 7}]}}

Fetch is like a dictionary get It returns the requested data only. Fields is a filter on a dictionary and leave the structure inplace but only reduces the data to the requested fields.

A more practical example: ::

    people = {'Jim': {'number': 13, 'age': 34},
              'Sam': {'number': 15, 'age': 45},
              'Tim': {'number': 18, 'age': 42},
              'Ric': {'number': 25, 'age': 25},
              'Eve': {'number': 32, 'age': 51}}

    fetch('*.age')
    >> [34, 45, 42, 25, 51]

    fields('*.age')
    >> {'Jim': {'age': 34},
        'Sam': {'age': 45},
        'Tim': {'age': 42},
        'Ric': {'age': 25},
        'Eve': {'age': 51}}

Find
----

Example ::

    people = {'Jim': {'number': 13, 'age': 34},
              'Sam': {'number': 15, 'age': 45},
              'Tim': {'number': 18, 'age': 42},
              'Ric': {'number': 25, 'age': 25},
              'Eve': {'number': 32, 'age': 51}}

    find({'*.number': 13})
    >> {'Jim': {'number': 13, 'age': 34}}

    find({'*.number': {'$lt': 15}})
    >> {'Jim': {'number': 13, 'age': 34}}

    find({'*.number': {'$lt': 16}})
    >> {'Jim': {'number': 13, 'age': 34},
        'Sam': {'number': 15, 'age': 45}}

