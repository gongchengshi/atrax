import unittest
import atrax.common.constants
import os
import aws.sns
atrax.common.constants.ControllerCrawlJobDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'input')
from atrax.common.crawl_job_notifications import CrawlJobNotifications


class CrawlJobNotificationsTest(unittest.TestCase):
    def test_init(self):
        target = CrawlJobNotifications('dummycrawljob')
        target.delete_all_topics()
        target = CrawlJobNotifications('dummycrawljob')
        target.initialize_topics()
        self.assertDictContainsSubset(target.topic_arns, target.topics)

    def test_delete_all_topics(self):
        target = CrawlJobNotifications('dummycrawljob')
        target.initialize_topics()
        target.delete_all_topics()
        actual = aws.sns.get_topic_arns(target.sns_conn, target.topics)
        self.assertEqual(len(actual), 0)

    def test_sync_notification_subscriptions(self):
        target = CrawlJobNotifications('dummycrawljob')
        target.sync_with_contact_list()

        actual = aws.sns.get_subscriptions_to_topic(target.sns_conn, target.stopping_topic_name)
        self.assertEqual(len(actual), 2)
        self.assertEqual(actual[0]['Endpoint'], 'gongchengshi@gmail.com')
        self.assertEqual(actual[1]['Endpoint'], '15095925898')

    def test_subscribe_unsubscribe(self):
        target = CrawlJobNotifications('dummycrawljob')
        target.subscribe_to_notifications()
        target.stopping_crawl_job()
        target.unsubscribe_all()
        target.stopping_crawl_job()

    def test_notification(self):
        target = CrawlJobNotifications('dummycrawljob')
        target.stopping_crawl_job()

    def test_unsubscribe_all(self):
        target = CrawlJobNotifications('dummycrawljob')
        target.unsubscribe_all()
        actual = aws.sns.get_confirmed_subscriptions_to_topic(target.sns_conn, target.stopping_topic_name)
        self.assertEqual(len(actual), 0)
