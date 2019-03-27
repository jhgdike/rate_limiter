# coding: utf-8

from __future__ import unicode_literals

import time
from datetime import datetime
from queue import Queue, Full, Empty
from threading import Thread


class BaseRateLimiter(object):

    def __init__(self, rate):
        self.rate = rate

    def acquire(self, count=1):
        raise NotImplementedError()


class ThreadingRateLimiter(BaseRateLimiter):

    def __init__(self, rate):
        super(ThreadingRateLimiter, self).__init__(rate)
        self.queue = Queue(rate)
        Thread(target=self._clear_queue).start()

    def acquire(self, count=1):
        try:
            self.queue.put(1, block=False)
        except Full:
            return False
        return True

    def _clear_queue(self):
        while True:
            time.sleep(1.0 / self.rate)
            try:
                self.queue.get(block=False)
            except Empty:
                pass


class DistributeRateLimiter(BaseRateLimiter):

    def __init__(self, rate, cache):
        super(DistributeRateLimiter, self).__init__(rate)
        self.cache = cache

    def acquire(self, count=1, expires=3, key=None, callback=None):
        try:
            if isinstance(self.cache, Cache):
                return self.cache.fetch_token(rate=self.rate, count=count, expires=expires, key=key)
        except Exception as ex:
            return True


class Cache(object):

    def __init__(self):
        self.key = 'default'
        self.namespace = 'ratelimiter'

    def fetch_token(self, *args, **kwargs):
        raise NotImplementedError()


class RedisTokenCache(Cache):

    def __init__(self, redis_instance):
        super(RedisTokenCache, self).__init__()
        self.redis = redis_instance

    def fetch_token(self, rate, count=1, expires=3, key=None):
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        key = ":".join([self.namespace, key if key else self.key, date])
        try:
            current = self.redis.get(key)
            if int(current if current else 0) > rate:
                return False
            else:
                with self.redis.pipeline() as p:
                    p.multi()
                    p.incr(key, count)
                    p.expire(key, int(expires if expires else 3))
                    p.execute()
                    return True
        except Exception as ex:
            return False
