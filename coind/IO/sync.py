from pymongo import MongoClient
from pymongo.collection import Collection
from functools import partial
from collections import Iterable
import logging
import re


def match(format, name):
    r = re.search(format, name, re.S)
    if r is None:
        return False
    else:
        return True


class SyncMapper(object):

    def __init__(self, source, target, source_db, target_db=None, col_map=None, match=None, collections=None):
        self.source = source
        self.target = target
        self.source_db = source_db
        self.target_db = target_db if target_db else source_db
        self.col_map = col_map if isinstance(col_map, dict) else {}
        if isinstance(match, str):
            self.match = [match]
        elif isinstance(match, Iterable):
            self.match = list(match)
        else:
            self.match = None
        self.collections = collections if isinstance(collections, list) else []

    @classmethod    
    def from_json(cls, filename):
        import json
        with open(filename) as f:
            configs = json.load(f)
        return cls(**configs)
    
    @classmethod
    def from_yaml(cls, filename):
        import yaml
        with open(filename) as f:
            configs = yaml.load(f)
        return cls(**configs)

    def pairs(self):
        db = MongoClient(self.source)[self.source_db]
        for source, target in self.find_cols(db):
            yield (self.source, self.source_db, source), (self.target, self.target_db, target)            

    def find_cols(self, db):
        if self.match:
            cols = self.matched_set(db)
        elif self.collections:
            cols = self.collections
        elif self.col_map:
            cols = self.col_map.keys()
        else:
            cols = db.collection_names()
        
        for col in cols:
            yield col, self.col_map.get(col, col)

    def matched_set(self, db):
        return set(self.iter_matched(db))

    def iter_matched(self, db):
        names = db.collection_names()
        for pattern in self.match:
            searcher = partial(match, pattern)
            yield from filter(searcher, names)


def get_col(host, db, col):
    return MongoClient(host)[db][col]


def sync(source, target, key, chunk):
    params = (source, target, key, chunk)
    s_col = get_col(*source)
    t_col = get_col(*target)
    result = key_pull(s_col, t_col, key, chunk)
    s_col.database.client.close()
    t_col.database.client.close()
    return result


def key_pull(source, target, key, chunk=10000):
    assert isinstance(source, Collection)
    assert isinstance(target, Collection)
    start = target.find_one(None, {key: 1}, sort=[(key, -1)])
    if start:
        flt = {key: {"$gt": start[key]}}
    else:
        flt = None
    cursor = source.find(flt)
    docs = []
    count = 0
    for doc in cursor:
        docs.append(doc)
        if len(docs) >= chunk:
            count = insert(target, docs, count)
            logging.warning("%s | %s", target.full_name, count)
            docs = []
    if len(docs):
        count = insert(target, docs, count)
        logging.warning("%s | %s", target.full_name, count)
    return count


def insert(collection, docs, count=0):
    r = collection.insert_many(docs).inserted_ids
    return count + len(r)


def main():
    from coind.utils.recycle import Recycler
    mapper = SyncMapper.from_yaml(r"D:\CoinData\guojin.yaml")
    params = [(source, target, "datetime", 1000) for source, target in mapper.pairs()]
        
    Recycler(sync).run(params, 3)

if __name__ == '__main__':
   main()
