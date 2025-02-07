import time
from functools import wraps

def get_time(func):
    """
    装饰器
    计算函数运行的时长
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(func.__name__, end-start)
        return result
    return wrapper


if __name__ == "__main__":
    
    @get_time
    def count(n):
        while n > 0:
            time.sleep(1)
            n -= 1

    count(2)