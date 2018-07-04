from coind.utils.vtObject import BarGenerator, parse_tick, VtBarData


class BarStorage(object):

    def __init__(self, method=None):
        self.on_fail = method

    def on_bar(self, bar):
        pass
    
    def flush(self):
        pass
    
    def last_bar(self):
        pass

    def set_on_fail(self, method):
        self.on_fail = method

class TickReader(object):

    def ranges(self, start=None, end=None):
        yield parse_tick({})


class FailedStack(object):

    def put(self, bar):
        pass
    
    def iter_bars(self):
        return []


class BarWriter(object):

    def __init__(self, reader, storage, failed=None):
        assert isinstance(reader, TickReader)
        assert isinstance(storage, BarStorage)
        self.reader = reader
        self.storage = storage
        self.failed = failed if isinstance(failed, FailedStack) else FailedStack()
        self.generager = BarGenerator(self.storage.on_bar)
        self.storage.set_on_fail(self.failed.put)

    def run(self, start=None, end=None):
        if start is None:
            last_bar = self.storage.last_bar()
            if isinstance(last_bar, VtBarData):
                start = last_bar.datetime
        
        for tick in self.reader.ranges(start, end):
            self.generager.updateTick(tick)

        self.storage.flush()
        self.redo_failed()    
    
    def redo_failed(self):
        for bar in self.failed.iter_bars():
            self.storage.on_bar(bar)