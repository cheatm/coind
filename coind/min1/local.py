from coind.min1.base import FailedStack


class LocalFailedStack(FailedStack):

    def __init__(self, filename):
        self.filename = filename
    
