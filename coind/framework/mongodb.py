from coind.framework.base import ContractDaily, DailyIndex, BaseConf
from pymongo import MongoClient
from pymongo.collection import Collection
import pandas as pd


class MongodbConf(BaseConf):

    @classmethod
    def from_conf(cls, host, col):
        client = MongoClient(host)
        db, collection = col.split(".", 1)
        return cls(client[db][collection])

    def __init__(self, collection):
        assert isinstance(collection, Collection)
        self.collection = collection
    
    def targets(self):
        return self.collection.find_one({"key": "targets"}, {"value": 1})["value"]


class MongodbContracts(ContractDaily):

    @classmethod
    def from_conf(cls, host, col):
        client = MongoClient(host)
        db, collection = col.split(".", 1)
        return cls(client[db][collection])

    def __init__(self, collection):
        assert isinstance(collection, Collection)
        self.collection = collection
    
    def init(self):
        self.collection.create_index(
            "date", background=True, unique=True
        )
    
    def write(self, date, contracts=None):
        if contracts:
            self.collection.update_one({"date": date}, {"$set": {"contracts": contracts}}, upsert=True)
        else:
            self.collection.update_one({"date": date}, {"$setOnInsert": {"date": date}}, upsert=True)

    def create(self, dates):
        for date in dates:
            self.write(date)
    
    def set(self, date, contracts):
        self.write(date, contracts)
    
    def get_contracts(self, date):
        contracts = self.collection.find_one({"date": date}, {"contracts": 1}).get("contracts", list())
        return set(contracts)
    
    def empty(self, dates):
        flt = {
            "date": {"$in": dates},
            "contracts": {"$exists": False}
        }
        docs = self.collection.find(flt, {"date": 1})
        return [doc["date"] for doc in docs]

    def check(self, date):
        flt = {
            "date": date,
            "contracts": {"$exists": True}
        }
        return self.collection.find(flt).count()


class MongodbIndex(DailyIndex):

    @classmethod
    def from_conf(cls, host, col, tag):
        client = MongoClient(host)
        db, collection = col.split(".", 1)
        return cls(client[db][collection], tag)

    def __init__(self, collection, tag=""):
        assert isinstance(collection, Collection)
        self.collection = collection
        self.tag = tag

    def init(self):
        self.collection.create_index(
            [("contract", 1), ("date", 1)],
            background=True,
            unique=True
        )

    def create(self, date, contracts):
        for contract in contracts:
            self._create(contract, date)
    
    def _create(self, contract, date):
        flt = {"contract": contract, "date": date}
        doc = {"length": 0, "tick": 0, "bar": 0, "lock": False}
        doc.update(flt)
        self.collection.update_one(flt, {"$setOnInsert": doc}, upsert=True)

    def get(self, contract, date):
        return self.collection.find_one({"contract": contract, "date": date}, {"_id": 0})
    
    def find(self, coutracts, dates, **kwargs):
        flt = {
            "contract": {"$in": coutracts},
            "date": {"$in": dates}
        }
        flt.update(kwargs)

        docs = list(self.collection.find(flt, {"_id": 0, "lock": 0}))
        return pd.DataFrame(docs)
    
    def last_date(self):
        return self.collection.find_one(sort=[("date", -1)])["date"]


frames = {
    "contracts": MongodbContracts,
    "index": MongodbIndex,
    "conf": MongodbConf 
}


def generate(configs):
    results = {}
    indexes = configs.get("indexes", {})
    for name, args in indexes.items():
        cls = frames[name]
        results[name] = cls.from_conf(**args)
    return results
            