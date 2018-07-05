import pandas as pd


class BaseConf(object):

    def targets(self):
        return list()
    

class ContractDaily(object):

    def create(self, dates):
        pass

    def set(self, date, contracts):
        pass
    
    def get_contracts(self, date):
        return set()
    
    def check(self, date):
        return False
    
    def empty(self, dates):
        return []
    

class DailyIndex(object):

    def create(self, date, contracts):
        return (0, 0)

    def lock(self, contract, date):
        return False
    
    def unlock(self, contract, date):
        return False
    
    def get(self, contract, date):
        return dict()
    
    def set(self, contract, date, length=0, tick=0, bar=0):
        return False
    
    def find(self, coutracts, dates, locked=False, **kwargs):
        return pd.DataFrame()

    def last_date(self):
        return ""


class TickStorage(object):

    def write(self, contract, frame):
        pass
    
    def check(self, contract, date):
        return 0
    
    

