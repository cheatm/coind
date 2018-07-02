from coind.hist.base import Storage, Indexer
from datautils.mongodb import update_chunk, insert_chunk, read_chunk, insert, update, read
from datautils.tools.logger import logger


DATE = "_d"
START = "_s"
END = "_e"

COLUMNS = ["open", "high", "low", "close", "volume", "amount"]


class MongodbStorage(Storage):

    def __init__(self, db):
        self.db = db

    def update(self, contract, date, data):
        return update_chunk(
            self.db[contract], data, DATE,
            {DATE: date, START: data.index[0], END: data.index[-1]}
        )

    def append(self, contract, date, data):
        return update_chunk(
            self.db[contract], data, DATE,
            {DATE: date, START: data.index[0], END: data.index[-1]},
            how="$setOnInsert"
        )

    def insert(self, contract, date, data):
        return insert_chunk(
            self.db[contract], data,
            {DATE: date, START: data.index[0], END: data.index[-1]}
        )

    def check(self, contract, date):
        return self.db[contract].find({DATE: date}).count()

    def read(self, contract, date):
        return read_chunk(self.db[contract], {"_d": date}, COLUMNS, "time")


class MongodbIndexer(Indexer):

    def __init__(self, collection):
        self.collection = collection
        self.collection.create_index([("contract", 1), ("date", 1)])

    def find(self, *contracts, start=None, end=None, **value):
        value["date"] = (start, end)
        if len(contracts):
            value["contract"] = list(contracts)
        return read(self.collection, ["contract", "date"], ["1M"], **value).sort_index()

    def update(self, frame):
        return update(self.collection, frame)

    def insert(self, frame):
        return insert(self.collection, frame)

    def append(self, frame):
        return update(self.collection, frame, how="$setOnInsert")