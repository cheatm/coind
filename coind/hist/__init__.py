from itertools import product
from coind.hist.base import His1min
from coind.hist.mongodb import MongodbIndexer, MongodbStorage


EXCHANGE = "okex"
BASIS = ["usdt", "btc", "eth"]
TYPES = ["btc", "ltc", "eth", "etc", "bch", "eos", "xrp"]

CONTRACTS = ["{}/{}.{}".format(EXCHANGE, t, b) for t, b in filter(lambda item: item[0] != item[1], product(TYPES, BASIS))]


from pymongo import MongoClient
from datetime import datetime


def t():
    client = MongoClient("192.168.0.102")
    indexer = MongodbIndexer(client["log"]["DIGICCY"])
    storage = MongodbStorage(client["DIGICCY"])
    handler = His1min(indexer, storage)
    handler.create_index(*CONTRACTS, start="2018-03-01", end="2018-05-19", how="insert")
    handler.write(*CONTRACTS, how="insert")



if __name__ == '__main__':
    t()