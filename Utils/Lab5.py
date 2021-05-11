import collections
import functools
import itertools
import json

def readData(filename):
    with open(filename, mode='r', encoding='utf-8') as file:
        for line in file:
            record = json.loads(line)
            yield(record)


class MapReduce:
    def __init__(self, mapper, reducer, num_workers=0):
        self.m = mapper
        self.r = reducer

    def __call__(self, inputs):
        # appliquer
        map_values = map(self.m, inputs)
        items = collections.defaultdict(list)
        for k,v in itertools.chain.from_iterable(map_values):
            items[k].append(v)

        reduce_values = map(self.r, items.items())

        return list(reduce_values)
