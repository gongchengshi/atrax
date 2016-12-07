from unittest import TestCase
from atrax.fetcher.redundant_url_tree import RedundantUrlTree


class RedundantUrlTreeTest(TestCase):
    def test_path_redundancy(self):
        target = RedundantUrlTree(path_support_threshold=10, file_support_threshold=20, param_support_threshold=2, confidence=1)

        target.insert_redundant("http://www.example.com/path1/path1")
        target.insert_redundant("http://www.example.com/path1/path2")
        target.insert_redundant("http://www.example.com/path1/path3")
        target.insert_redundant("http://www.example.com/path1/path4")
        target.insert_redundant("http://www.example.com/path1/path5")
        target.insert_redundant("http://www.example.com/path1/path6")
        target.insert_redundant("http://www.example.com/path1/path7")
        target.insert_redundant("http://www.example.com/path1/path8")
        target.insert_redundant("http://www.example.com/path1/path9")
        self.assertFalse(target.is_redundant("http://www.example.com/path1/path5"))
        target.insert_redundant("http://www.example.com/path1/path4")
        self.assertTrue(target.is_redundant("http://www.example.com/path1/path5"))
        target.insert_non_redundant("http://www.example.com/path1/path6")
        self.assertFalse(target.is_redundant("http://www.example.com/path1/path5"))

    def test_path_non_redundancy(self):
        target = RedundantUrlTree(path_support_threshold=10, file_support_threshold=20, param_support_threshold=2, confidence=1)

        target.insert_non_redundant("http://www.example.com/path1/path1")
        target.insert_non_redundant("http://www.example.com/path1/path1")
        target.insert_redundant("http://www.example.com/path1/path2")
        target.insert_redundant("http://www.example.com/path1/path3")
        target.insert_redundant("http://www.example.com/path1/path4")
        target.insert_redundant("http://www.example.com/path1/path5")
        target.insert_redundant("http://www.example.com/path1/path6")
        target.insert_redundant("http://www.example.com/path1/path7")
        target.insert_redundant("http://www.example.com/path1/path8")
        target.insert_redundant("http://www.example.com/path1/path9")
        target.insert_redundant("http://www.example.com/path1/path10")
        target.insert_redundant("http://www.example.com/path1/path11")
        self.assertFalse(target.is_redundant("http://www.example.com/path1/path12"))

        target.insert_redundant("http://www.example.com/path2/path1")
        target.insert_redundant("http://www.example.com/path3/path1")
        target.insert_redundant("http://www.example.com/path4/path1")
        target.insert_redundant("http://www.example.com/path5/path1")
        target.insert_redundant("http://www.example.com/path6/path1")
        target.insert_redundant("http://www.example.com/path7/path1")
        target.insert_redundant("http://www.example.com/path8/path1")
        target.insert_redundant("http://www.example.com/path9/path1")
        target.insert_redundant("http://www.example.com/path10/path1")
        target.insert_redundant("http://www.example.com/path11/path1")
        self.assertFalse(target.is_redundant("http://www.example.com/path12/path1"))

        target.insert_redundant("http://www.example.com/path8/path1")
        target.insert_redundant("http://www.example.com/path8/path1")
        target.insert_redundant("http://www.example.com/path8/path1")
        target.insert_redundant("http://www.example.com/path8/path1")
        target.insert_redundant("http://www.example.com/path8/path1")
        target.insert_redundant("http://www.example.com/path8/path1")
        target.insert_redundant("http://www.example.com/path8/path1")
        target.insert_redundant("http://www.example.com/path8/path1")
        target.insert_redundant("http://www.example.com/path8/path1")
        self.assertFalse(target.is_redundant("http://www.example.com/path9/path1"))
        self.assertTrue(target.is_redundant("http://www.example.com/path8/path1"))

    def test_params_redundancy(self):
        target = RedundantUrlTree(path_support_threshold=10, file_support_threshold=4, param_support_threshold=2, confidence=1)

        target.insert_redundant("http://www.example.com/path1/path1?p1=1&p2=2")
        self.assertFalse(target.is_redundant("http://www.example.com/path1/path1?p1=1&p4=4"))
        target.insert_redundant("http://www.example.com/path1/path1?p1=1&p3=3")
        self.assertTrue(target.is_redundant("http://www.example.com/path1/path1?p1=1&p4=4"))
        target.insert_non_redundant("http://www.example.com/path1/path1?p1=1&p4=4")
        self.assertFalse(target.is_redundant("http://www.example.com/path1/path1?p1=1&p4=4"))

    def test_params_non_redundancy(self):
        target = RedundantUrlTree(path_support_threshold=50, file_support_threshold=50, param_support_threshold=2, confidence=1)

        target.insert_non_redundant("http://www.example.com/path1/path1?p1=1&p2=1")
        target.insert_non_redundant("http://www.example.com/path1/path1?p1=1&p2=1")
        target.insert_redundant("http://www.example.com/path1/path1?p1=1&p2=2")
        target.insert_redundant("http://www.example.com/path1/path1?p1=1&p2=3")
        target.insert_redundant("http://www.example.com/path1/path1?p1=1&p2=4")
        self.assertFalse(target.is_redundant("http://www.example.com/path1/path1?p1=1&p2=5"))
