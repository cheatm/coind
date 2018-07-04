

class ContractDaily(object):

    def set(self, date, contracts=None, targets=None):
        pass
    
    def get_target(self, date):
        pass

    def get_contracts(self, date):
        pass


class DailyIndex(object):

    def create(self, date, contracts):
        pass

    def lock(self, contract, date):
        pass
    
    def unlock(self, contract, date):
        pass
    
    def get(self, contract, date):
        pass
    
    def set(self, contract, date, length=0, tick=0, bar=0):
        pass
    

class TickStorage(object):

    def write(self, contract, frame):
        pass
    
    def read(self, contract, date):
        pass
    
    def check(self, contract, date):
        pass

