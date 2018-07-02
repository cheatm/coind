import logging
import multiprocessing
from functools import wraps


class Recycler(object):

    def __init__(self, function):
        self.function = function
        self.failed = []
    
    def callback(self, result):
        if result.get("ok", False):
            logging.warning("%s | %s", result.get("params", None), result.get("result", None))
        else:
            if "params" in result:
                logging.error("%s | %s", result["params"], result.get("error", None))
                self.failed.append(result["params"])

    def error_callback(self, error):
        print(error)

    def run(self, params, times=1, wrap=True, **kwargs):
        times -= 1
        if wrap:
            method = self.wrapped
        else:
            method = self.function
        pool = multiprocessing.Pool(**kwargs)
        for param in params:
            pool.apply_async(method, param, callback=self.callback, error_callback=self.error_callback)
        pool.close()
        pool.join()
        logging.warning("Tasks done | total: %s | fail: %s | retry: %s", len(params), len(self.failed), times)
        if self.failed and times:
            failed = tuple(self.failed)
            self.failed = []
            self.run(failed, times, **kwargs)
        else:
            logging.warning("Tasks stop | failed: %s", self.failed)

    def wrapped(self, *args):
        try:
            result = self.function(*args)
        except Exception as e:
            return {"error": e, "ok": False, "params": args}
        else:
            return {"ok": True, "params": args, "result": result}
