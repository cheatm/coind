from coind.min1.base import BarStorage, TickReader, BarWriter, parse_tick, VtBarData
from pymongo.collection import Collection
from pymongo import UpdateOne
import logging


def parser(**filters):
    dct = {}
    for key, value in filters.items():
        if "$" in key:
            dct[key] = value
        else:
            if isinstance(value, tuple):
                r = parse_range(*value)
                if len(r):
                    dct[key] = r
            elif isinstance(value, (list, set)):
                dct[key] = {"$in": list(value)}
            else:
                dct[key] = value
    return dct


def parse_range(start=None, end=None):
    dct = {}
    if start:
        dct["$gte"] = start
    if end:
        dct["$lte"] = end
    return dct


class MongodbTickReader(TickReader):

    def __init__(self, collection):
        assert isinstance(collection, Collection)
        self.collection = collection
    
    def ranges(self, start=None, end=None):
        filters = parser(datetime=(start, end))
        cursor = self.collection.find(filters).sort([("datetime", 1)])
        for doc in cursor:
            yield parse_tick(doc)
    

import pandas as pd


class MongodbBarStorage(BarStorage):

    def __init__(self, collection, tag=1800):
        super(MongodbBarStorage, self).__init__()
        assert isinstance(collection, Collection)
        self.collection = collection
        if self.collection.find().count() == 0:
            self.method = self.insert
        else:
            self.ensure_index()
            self.method = self.update
        self.tag = tag
        self.count = 0

    def ensure_index(self):
        indexes = self.collection.index_information()
        if "datetime_1" not in indexes:
            self.collection.create_index("datetime", background=True)

    def update(self, bar):
        filters = {"datetime": bar["datetime"]}
        self.collection.update_one(filters, {"$set": bar}, upsert=True)
    
    def insert(self, bar):
        self.collection.insert_one(bar)

    def on_bar(self, bar):
        bar = bar.__dict__
        try:
            self.method(bar)
        except Exception as e:
            logging.error("write error | %s | %s", bar, e)
            self.on_fail(bar)
        self.count += 1
        if self.count % self.tag == 0:
            logging.warning("bar | %s | %s", self.collection.name, self.count)
            
    def last_bar(self):
        doc = self.collection.find_one(sort=[("datetime", -1)])
        if isinstance(doc, dict):
            bar = VtBarData()
            bar.__dict__ = doc
            return bar
        else:
            return None
    
    def flush(self):
        logging.warning("bar | %s | %s", self.collection.name, self.count)
        self.ensure_index()


def make_update(doc):
    return UpdateOne({"datetime", doc["datetime"]}, doc, upsert=True)


TICK = "VnTrader_Tick_Db"
BAR = "VnTrader_1Min_Db"


def ex_end(s):
    for tag in ["week", "quarter"]:
        if tag in s:
            return False 
    for ex in ["okex", "huobip", "binance"]:
        if s.endswith(ex):
            return True
    return False


def is_extra(s):
    for contract in ["xrp", "xlm", "ada", "iota"]:
        if contract in s:
            return True
    return False


def main():
    from coind.utils.recycle import Recycler

    recycler = Recycler(min1store)
    names = [(name,) for name in filter(is_extra, MongoClient("192.168.0.101:37017")[TICK].collection_names())]
    # print(names)
    recycler.run(names, 3)


from pymongo import MongoClient
from datetime import datetime


def min1store(SYMBOL):

    # SYMBOL = "btc.usdt:xtc.binance"
    logging.warning("Start making %s", SYMBOL)
    client = MongoClient("192.168.0.101:37017")
    tick = client[TICK][SYMBOL]
    bar = client[BAR][SYMBOL]

    reader = MongodbTickReader(tick)
    storage = MongodbBarStorage(bar)
    writer = BarWriter(reader, storage)
    writer.run(datetime(2018, 3, 1), None)
    return storage.count
    

if __name__ == '__main__':
    main()