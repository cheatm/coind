from datautils.tools.logger import logger
from itertools import product
from coind.IO.history import history_1min
from coind.IO.contracts import list_contracts
from datetime import datetime
import pandas as pd



class His1min(object):

    def __init__(self, indexer, storage):
        self.indexer = indexer
        self.storage = storage

    def write(self, *contracts, start=None, end=None, ignore_index=False, how="update"):
        if ignore_index:
            assert start, "start is None"
            assert end, "end is None"
            dates = map(str, date_list(start, end))
            sequence = product(contracts, dates)
        else:
            sequence = self.indexer.find(*contracts, start=start, end=end, **{"1M": 0}).index

        for contract, date in sequence:
            self.storage.write(contract, date, how)

    def check(self, *contracts, start=None, end=None, cover=False):
        if cover:
            table = self.indexer.find(*contracts, start=start, end=end, fields="1M")
        else:
            table = self.indexer.find(*contracts, start=start, end=end, fields="1M", **{"1M": 0})
        for contract, date in table.index:
            result = self.storage.check(contract, date)
            table.loc[(contract, date), "1M"] = result
        self.indexer.update(table)

    def create_index(self, *contracts, start=None, end=None, how="append"):
        return self.indexer.create(start, end, *contracts, how)


    def update_index(self, *contracts, date=datetime.today().strftime("%Y-%m-%d")):
        return self.indexer.create(date, date, *contracts, "append")


class Storage(object):

    def read(self, contract, date):
        pass

    def check(self, contract, date):
        return 0

    def update(self, contract, date, data):
        pass

    def insert(self, contract, date, data):
        pass

    def append(self, contract, date, data):
        pass

    @logger("write min1", 1, 2)
    def write(self, contract, date, how="update"):
        data = history_1min(contract, date)
        assert len(data.index) > 0, "length of data <= 0"
        return getattr(self, how)(contract, str2date(date), data)


class Indexer(object):

    def find(self, *contracts, start=None, end=None, fields=None, **value):
        pass

    @logger("index", 1, 2)
    def create(self, start, end, *contracts, how="append"):
        dates = list(map(lambda t: t.strftime("%Y-%m-%d"), date_list(start, end)))
        table = list_contracts(dates, contracts).rename_axis(str2date)
        index = table.stack().index
        frame = pd.DataFrame(0, index, ["1M"])
        frame.index.names = ["date", "contract"]
        return getattr(self, how)(frame)

    def update(self, frame):
        pass

    def insert(self, frame):
        pass

    def append(self, frame):
        pass


def str2date(time):
    if isinstance(time, datetime):
        return time
    else:
        return datetime.strptime(time, "%Y-%m-%d")


def date_list(start, end):
    return pd.date_range(start, end)

