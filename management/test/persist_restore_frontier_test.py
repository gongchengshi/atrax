import unittest
from aws import USWest2 as AwsConnections


class PersistRestoreFrontierTest(unittest.TestCase):
    def setUp(self):
        AwsConnections.sqs()

    def test_persist_frontier(self):
        pass

    def test_restore_frontier(self):
        pass

    def test_delete_persisted_frontier(self):
        pass

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()