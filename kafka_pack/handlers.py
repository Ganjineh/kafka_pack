from pymemcache.client import base

memcache_client = base.Client(('127.0.0.1', 11211))


def set_in_cache(key, value, ttl):
    memcache_client.set(key, value, ttl)


def get_from_cache(key):
    res = memcache_client.get(key,"")
    if isinstance(res, bytes):
        return res.decode('utf-8')
    return res


def del_from_cache(key):
    memcache_client.delete(key)


def is_in_cache(key):
    value = memcache_client.get(key)
    if value is None:
        return False
    else:
        return True

