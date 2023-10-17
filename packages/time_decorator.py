from functools import wraps
import logging
import time
from typing import Callable


def timeit(func: Callable) -> Callable:
    """Decorator to measure time of execution of a function

    Args:
        func (Callable): function to be measured

    Returns:
        Callable: measured function
    """

    @wraps(func)
    def timeit_wrapper(*args, **kwargs) -> Callable:
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        logging.info(f"Function {func.__name__} took {total_time:.2f} seconds")
        return result

    return timeit_wrapper
