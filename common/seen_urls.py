import pyreBloom
import redis
from redis.exceptions import RedisError
import time
from atrax.common.constants import DEFAULT_REDIS_PORT, LOCALHOST_IP
from atrax.common import domain_from_schemeless_url


def extract_seen_url_set_key_from_url_id(url_id):
    return domain_from_schemeless_url(url_id)


class SeenUrls:
    def __init__(self, base_key, host=LOCALHOST_IP, port=DEFAULT_REDIS_PORT):
        self._base_key = base_key + '_'
        self._host = host
        self._port = port
        self._redis = redis.Redis(host, port)
        self._bloom_filters = {}

    def _bloom_filter(self, set_key):
        key = self._base_key + set_key
        try:
            bloom_filter = self._bloom_filters[key]
        except KeyError:
            # It is expected that 10 in 1,000,000 URLs will be false positives
            # There is a bug in pyreBloom that makes it segfault when key is not a byte string.
            bloom_filter = pyreBloom.pyreBloom(key.encode(), 1000000, 0.00001, host=self._host, port=self._port)
            self._bloom_filters[key] = bloom_filter
        return bloom_filter

    def __contains__(self, value):
        """
        value[0] = url_id
        value[1] domain
        Returning True means the URL has probably been seen. There is a low
            probability of returning True when the URL has not actually been seen.
            Because of this small probability it is possible to miss a URL entirely.
        Returning False means that the URL has definitely not been seen.
        """
        while True:
            try:
                return self._bloom_filter(value[1]).contains(value[0])
            except pyreBloom.pyreBloomException, ex:
                if not ex.message .startswith('LOADING'):  # Redis is loading the dataset in memory
                    raise ex
                time.sleep(1)

    def add(self, url_id, set_key=None):
        if set_key is None:
            set_key = extract_seen_url_set_key_from_url_id(url_id)
        while True:
            try:
                self._bloom_filter(set_key).add(url_id)
                break
            except pyreBloom.pyreBloomException, ex:
                if not ex.message .startswith('LOADING'):  # Redis is loading the dataset in memory
                    raise ex
                time.sleep(1)

    def save(self):
        try:
            self._redis.bgsave()
        except RedisError, e:
            print 'Exception in Redis.bgsave(): %s' % e
