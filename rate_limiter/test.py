import time
from rate_limiter.limiter import ThreadingRateLimiter, DistributeRateLimiter, RedisTokenCache


def test_single():
    limiter = ThreadingRateLimiter(10)
    while True:
        time.sleep(0.05)
        if limiter.acquire():
            print('I acquire!')
        else:
            print("I don't acquire")


def test_distribute():
    from redis import Redis
    redis = Redis()
    cache = RedisTokenCache(redis)
    limiter = DistributeRateLimiter(10, cache)
    while True:
        time.sleep(0.05)
        if limiter.acquire():
            print('I acquire!')
        else:
            print("I don't acquire")


if __name__ == '__main__':
    test_distribute()
