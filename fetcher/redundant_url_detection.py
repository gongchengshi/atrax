import pickle
import redis
from python_common.math import is_power2
from atrax.common.constants import LOCALHOST_IP, DEFAULT_REDIS_PORT
from atrax.fetcher.redundant_url_tree import RedundantUrlTree


class RedundantUrlDetector:
    def __init__(self, redundant_urls_key, persist_interval=1024, host=LOCALHOST_IP, port=DEFAULT_REDIS_PORT):
        assert(is_power2(persist_interval))
        self._url_trees = {}  # Keyed by domain name
        self._redundant_urls_key = redundant_urls_key
        self._redis = redis.Redis(host, port)
        self._modulus_mask = persist_interval - 1
        # used to get the latest default parameters from the source code to apply to persisted trees
        self._default_tree_parameters = RedundantUrlTree()

    def persist_all(self):
        for set_key, tree in self._url_trees.iteritems():
            self._persist(set_key, tree)

    def _persist(self, set_key, tree):
        self._redis.hset(self._redundant_urls_key, set_key, pickle.dumps(tree, 2))

    def _persist_if_necessary(self, set_key, tree):
        if tree.count & self._modulus_mask == 0:
            self._persist(set_key, tree)

    def _get_tree(self, set_key):
        try:
            tree = self._url_trees[set_key]
        except KeyError:
            pickled_tree = self._redis.hget(self._redundant_urls_key, set_key)
            if pickled_tree is None:
                tree = RedundantUrlTree()  # Using default parameters
            else:
                tree = pickle.loads(pickled_tree)
                tree._path_support_threshold = self._default_tree_parameters._path_support_threshold
                tree._file_support_threshold = self._default_tree_parameters._file_support_threshold
                tree._param_support_threshold = self._default_tree_parameters._param_support_threshold
                tree._confidence = self._default_tree_parameters._confidence
            self._url_trees[set_key] = tree
        return tree

    def insert_redundant(self, url_info):
        tree = self._get_tree(url_info.domain)
        count = tree.count
        tree.insert_redundant(url_info.url)
        if tree.count > count:
            self._persist_if_necessary(url_info.domain, tree)

    def insert_non_redundant(self, url_info):
        tree = self._get_tree(url_info.domain)
        count = tree.count
        tree.insert_non_redundant(url_info.url)
        if tree.count > count:
            self._persist_if_necessary(url_info.domain, tree)

    def is_redundant(self, url_info):
        return self._get_tree(url_info.domain).is_redundant(url_info.url)
