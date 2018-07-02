import requests
from _io import BytesIO
import os
import pandas as pd
from gzip import GzipFile
import json
import click
import traceback


CONTRACTS = "http://alihz-net-0.qbtrade.org/contracts?date={}"
ROOT = "http://alihz-net-0.qbtrade.org/"
HIST_TICK = os.path.join(ROOT, "hist-ticks")

PRICE_MAP = {"high": "max",
             "low": "min",
             "open": "first",
             "close": "last"}


def retry(count=3, stop=True, default=None):
    def _retry(method):
        def wrapper(*args, **kwargs):
            for i in range(count):
                try:
                    return method(*args, **kwargs)
                except Exception as e:
                    print(e)
            if stop:
                raise e
            else:
                if default:
                    return default()            
                else:
                    return None
        return wrapper
    return _retry

            


def contracts_url(date):
    return CONTRACTS.format(date)


@retry(stop=False, default=list)
def get_contracts(date):
    url = contracts_url(date)
    content = requests.get(url).content
    return json.loads(content)


def hist_tick_url(contract, date):
    return "%s?%s" % (HIST_TICK, "date={}&contract={}".format(date, contract))


def history_tick_content(contract, date):
    response = requests.get(hist_tick_url(contract, date), stream=True)
    if response.status_code == 200:
        disposition = response.headers['Content-Disposition']
        bio = BytesIO(b"")
        chunk_size = 2**16
        with click.progressbar(response.iter_content(chunk_size), label=disposition) as bar:
            for content in bar:
                bio.write(content)
        bio.seek(0)
        return bio.read()
    else:
        raise IOError(response.status_code)


def tick_file_name(contract, date, root="."):
    name = "%s_%s.gz" % (contract.replace("/", '_'), date)
    return os.path.join(root, name)


@retry(stop=False, default=lambda: ".")
def save_tick(contract, date, root="."):
    f_name = tick_file_name(contract, date, root)
    if os.path.exists(f_name):
        return f_name
    content = history_tick_content(contract, date)
    with open(f_name, "wb") as f:
        f.write(content)
    return f_name


def history_tick(contract, date):
    content = history_tick_content(contract, date)
    bio = BytesIO(content)
    gf = GzipFile(fileobj=bio)
    return list(select(gf))


def make_1min(ticks):
    data = [{name: tick[name] for name in ["time", "volume", "last", "amount"]} for tick in ticks]
    frame = pd.DataFrame(data).set_index("time").rename_axis(pd.to_datetime)
    rs = frame.resample("1min", label="right", closed="left")
    result = rs["last"].agg(PRICE_MAP)
    result[['amount', 'volume']] = rs[['amount', 'volume']].sum()
    result.index.name = "time"
    return result


def history_1min(contract, date):
    ticks = history_tick(contract, date)
    return make_1min(ticks)


def select(gf):
    for line in gf.readlines():
        try:
            record = json.loads(line)
            if record["volume"] != 0:
                yield record
        except:
            pass


def read_tick(filename):
    if isinstance(filename, BytesIO):
        gf = GzipFile(fileobj=filename)
    elif isinstance(filename, bytes):
        gf = GzipFile(fileobj=BytesIO(filename))
    else:
        gf = GzipFile(filename)
    return list(select(gf))


def tick_frame(ticks):
    result = pd.DataFrame(list(map(structure, ticks)))
    return result.reindex_axis(sorted(result.columns, key=get_column_order), 1)


def structure(tick):
    asks = dict(pv('ask', tick["asks"]))
    bids = dict(pv('bid', tick["bids"]))
    dct = {key: tick[key] for key in ["amount", "contract", "last", "time", "volume"]}
    dct.update(asks)
    dct.update(bids)
    return dct


def pv(name, docs):
    count = 1
    for doc in docs:
        for key, value in doc.items():
            yield "%s_%s_%s" % (name, key, count), value
        count += 1


COLUMN_ORDER = {"amount": 3, "contract": 1, "last": 2, "time": 0, "volume": 4}
TICK_ORDER = {"bid": 1, "ask": 2, "price": 1, "volume": 2}


def get_column_order(column):
    try:
        return COLUMN_ORDER[column]
    except KeyError:
        direction, types, order = column.split("_", 2)
        return TICK_ORDER[direction]*1000+int(order)*10+TICK_ORDER[types]


def tick2csv(path=""):
    ticks = read_tick(path)
    frame = tick_frame(ticks)
    name, tag = path.rsplit(".", 1)
    f_name = "%s.csv" % name
    frame.to_csv(f_name)
    return frame


# 输入tick压缩文件路径 返回一分钟
def tick2min(path):
    ticks = read_tick(path)
    frame = make_1min(ticks)
    return frame


from threading import Thread
from queue import Queue, Empty


class TreadPool(object):

    def __init__(self, func, redo=False):
        self.queue = Queue()
        self.func = func
        self.redo = redo
        self.threads = {}
    
    def run(self, iterable, count=5):
        for item in iterable:
            self.queue.put(item)
        for i in range(count):
            t = Thread(target=self.execute)
            self.threads[i] = t
            t.start()
            t.join(0)

    def execute(self):
        while self.queue.qsize():
            try:
                item = self.queue.get_nowait()
            except Empty:
                pass
            
            try:
                self.func(item)
            except Exception as e:
                print(item, e)
                if self.redo:
                    self.queue.put(item)
            
            

def download(item):
    contract, date = item
    f_name = save_tick(contract, date)
    tick2csv(f_name)
    frame = tick2min(f_name)
    frame.to_excel("%s_1min.xlsx" % f_name.rsplit(".", 1)[0])


if __name__ == '__main__':
    from itertools import product
    # os.chdir(r"D:\CoinData\tick")
    # tp = TreadPool(download, True)
    # tp.run(product(
    #     ["okex/bch.usdt", "okex/eth.usdt", "okex/xrp.usdt", "okex/bch.usdt", "okex/eos.usdt"], 
    #     pd.date_range("2018-01-01", "2018-06-05").map(lambda t: t.strftime("%Y-%m-%d"))
    # ))
    print(hist_tick_url("okex/bch.usdt", "2018-06-05"))
