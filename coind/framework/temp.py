from coind.framework.base import ContractDaily, DailyIndex, BaseConf
import pandas as pd


class TempConf(BaseConf):

    def set_targets(self, targets):
        pass

    def targets(self):
        return ['binance/eth.btc', 'okex/ltc.btc', 'huobip/xrp.usdt', 'binance/ada.usdt', 'huobip/ltc.usdt', 'huobip/bch.usdt', 'binance/btc.usdt', 'binance/eos.usdt', 'binance/xlm.usdt', 'okex/eos.usdt', 'okex/btc.usdt', 'huobip/ada.usdt', 'okex_test/btc.quarter', 'binance/bch.btc', 'okex/eth.usdt', 'huobip/ht.btc', 'binance/ltc.usdt', 'okex/xrp.usdt', 'huobip/ht.usdt', 'binance/eos.btc', 'okex/ltc.usdt', 'huobip/ltc.btc', 'okex/bch.usdt', 'binance/ltc.btc', 'okex/eth.btc', 'huobip/btc.usdt', 'binance/bnb.btc', 'huobip/eos.btc', 'huobip/bch.btc', 'huobip/eos.usdt', 'binance/bch.usdt', 'okex/bch.btc', 'binance/bnb.usdt', 'huobip/iota.usdt', 'okex/iota.usdt', 'binance/iota.usdt', 'binance/xrp.usdt', 'okex/eos.btc', 'okex/btc.quarter', 'binance/eth.usdt', 'huobip/eth.usdt', 'huobip/eth.btc', 'okex/xlm.usdt']


class TempContractDaily(ContractDaily):

    def __init__(self):
        self.contracts = {}
    
    def create(self, dates):
        for date in dates:
            self.contracts[date] = []

    def set(self, date, contracts):
        self.contracts[date] = contracts
    
    def get_contracts(self, date):
        return set(self.contracts[date])
    
    def check(self, date):
        return date in self.contracts
    
    def empty(self, dates):
        return list(self._empty(dates))

    def _empty(self, dates):
        for date in dates:
            if date not in self.contracts:
                yield date
            elif len(self.contracts[date]) == 0:
                yield date


class TempDailyIndex(DailyIndex):

    def __init__(self):
        self.index = {}

    def create(self, date, contracts):
        for contract in contracts:
            self.set(contract, date)
    
    def set(self, contract, date, length=0, tick=0, bar=0):
        self.index.setdefault(date, {})[contract] = {
            "contract": contract,
            "date": date,
            "length": length,
            "tick": tick,
            "bar": bar
        }
    
    def get(self, contract, date):
        return self.index.get(date, {}).get(contract, {})

    def find(self, coutracts, dates, locked=False, **kwargs):
        logs = []
        for date in dates:
            for contract in coutracts:
                log = self.get(contract, date)
                if log:
                    logs.append(log)
        return pd.DataFrame(logs)

    def last_date(self):
        return list(sorted(self.index))[-1]