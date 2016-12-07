from unittest import TestCase
from atrax.frontier.frontier_consumer import FrontierConsumer


class FrontierConsumerTest(TestCase):
    def test_dequeue_rate(self):
        target = FrontierConsumer(0)
        target.queues = [1, 2, 3, 4, 5]
        target._cycle_interval = 1

        print target.per_queue_dequeue_rate
