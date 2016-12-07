import random
import urlparse
import xxhash
from pybst.bstree import BSTree


class Node:  # Pickle can't pickle nested classes so this needs to be out here.
    def __init__(self):
        self.support = 0  # The number of times this node has been involved in a redundant URL.
        # This is just a compact way of storing/searching a list of child nodes. This is not the tree itself.
        self._children = BSTree()

    def get_child(self, key):
        node = self._children.get_node(key)
        return None if node is None else node.value

    def add_child(self, key):
        node = Node()
        self._children.insert(key, node)
        return node


class RedundantUrlTree:
    # Todo: How low can these thresholds be and still have acceptable confidence of little to no false positives?
    # These parameters are just educated guesses.
    def __init__(self, path_support_threshold=100, file_support_threshold=20,
                 param_support_threshold=10, confidence=.95):
        """
        :param path_support_threshold: If <param_support_threshold> URLs with the same sub path are all redundant
        then all URLs in that sub path will be considered redundant henceforth.
        This is the least sensitive estimator and should be set high.
        :param file_support_threshold: If <file_support_threshold> URLs with the exact same path are all redundant
        then all URLs in that exact path will be considered redundant henceforth.
        This is the second most sensitive estimator and can be set close to the param_support_threshold.
        :param param_support_threshold: If <param_support_threshold> URLs with the same path all share at least one
        parameter name in common and they are all redundant then all URLs in that path that contain the parameter
        name will be considered redundant henceforth. This is the most sensitive estimator and should be set the lowest.
        :param confidence: The measure of how confident the user is that the thresholds are correct. With a confidence
        of less than 1.0 is_redundant() will return False at a rate of 1 - confidence on a random basis, regardless of
        the amount of support for redundancy. This does not affect the existing support values. Afterwards if the URL
        is found, in actuality, to not be redundant then the client of this class should specify this using the
        insert_non_redundant() method.
        """
        assert(path_support_threshold >= 10)
        assert(file_support_threshold >= 2)
        assert(param_support_threshold >= 2)
        assert(0 < confidence <= 1.0)

        self._root = Node()
        self._path_support_threshold = path_support_threshold
        self._file_support_threshold = file_support_threshold
        self._param_support_threshold = param_support_threshold
        self._confidence = confidence
        self._count = 0

    @property
    def count(self):
        return self._count

    def insert_redundant(self, url):
        path_tokens, param_tokens = RedundantUrlTree.tokenize(url)
        node = self._root
        new = False

        for token in path_tokens:
            child_node = node.get_child(token)
            if child_node is None:
                child_node = node.add_child(token)
                new = True
            node = child_node
            if node.support >= 0:  # Don't overwrite fact that this node cannot signify redundancy
                node.support += 1

        for token in param_tokens:
            param_node = node.get_child(token)
            if param_node is None:
                param_node = node.add_child(token)
                new = True
            if param_node.support >= 0:
                param_node.support += 1

        if new:
            self._count += 1

    def insert_non_redundant(self, url):
        path_tokens, param_tokens = RedundantUrlTree.tokenize(url)
        node = self._root
        new = False

        for token in path_tokens:
            # It's necessary to add non-redundant URLs to capture the possibility that none of the
            # tokens in the URL can be used to signify redundancy in future URLs.
            child_node = node.get_child(token)
            if child_node is None:
                node = node.add_child(token)  # support remains at 0
                new = True
            else:
                # If the node is not None then it must have been added before which means that it
                # cannot possibly ever signify redundancy. Setting support to -1 works like a flag to this end.
                child_node.support = -1
                node = child_node

        for token in param_tokens:
            param_node = node.get_child(token)
            if param_node is None:
                node.add_child(token)
                new = True
            else:
                param_node.support = -1

        if new:
            self._count += 1

    def is_redundant(self, url):
        path_tokens, param_tokens = RedundantUrlTree.tokenize(url)

        node = self._root
        for token in path_tokens:
            node = node.get_child(token)
            if node is None:
                return False
            elif node.support >= self._path_support_threshold:
                return self._true_with_confidence_adjustment()

        if node.support >= self._file_support_threshold:
            return self._true_with_confidence_adjustment()

        for token in param_tokens:
            param_node = node.get_child(token)
            if param_node is not None and param_node.support >= self._param_support_threshold:
                return self._true_with_confidence_adjustment()

        return False

    def _true_with_confidence_adjustment(self):
        if self._confidence >= 1:
            return True

        return random.random() <= self._confidence

    @staticmethod
    def hash(key):
        # xxHash was picked over Murmur and Adler because it's faster according to literature.
        return xxhash.xxh64(key).intdigest()  # Use only on 64 bit platforms

    @staticmethod
    def tokenize(url):
        parts = urlparse.urlsplit(url)

        path_tokens = [hash(parts.netloc)] + [hash(part) for part in parts.path.strip('/').split('/')]
        param_tokens = [hash(part) for part in urlparse.parse_qs(parts[3]).keys()]

        return path_tokens, param_tokens
