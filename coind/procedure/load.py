from coind.framework.base import ContractDaily, DailyIndex, TickStorage


class LoadData(object):

    def __init__(self, contracts, index, storage, ticks):
        assert isinstance(contracts, ContractDaily)
        assert isinstance(index, DailyIndex)
        assert isinstance(ticks, TickStorage)
        self.contracts = contracts
        self.index = index
        self.ticks = ticks
    
    def tick(self, contracts=None, start=None, end=None):
        table = self.index.find(contracts, start, end)
        
    
    def bar(self):
        pass