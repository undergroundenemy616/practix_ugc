import logging
import tracemalloc
from functools import wraps


def measure_memory(func):
    tracemalloc.start()
    func()
    current, peak = tracemalloc.get_traced_memory()
    logging.info(f"Function Name       : {func.__name__}")
    logging.info(f"Current memory usage: {current / 10**6}MB")
    logging.info(f"Peak                :  {peak / 10**6}MB")
    tracemalloc.stop()


def coroutine(func):
    @wraps(func)
    def inner(*args, **kwargs):
        fn = func(*args, **kwargs)
        next(fn)
        return fn

    return inner
