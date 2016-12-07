from unittest import TestCase

from atrax.fetcher.redundant_url_detection import RedundantUrlDetector
from atrax.common.crawl_job import CrawlJobGlossary
from atrax.common.constants import DEFAULT_REDIS_PORT
from atrax.common.url_info import UrlInfo


class RedundantUrlDetectorTest(TestCase):
    def test_store_large_tree(self):
        crawl_job_glossary = CrawlJobGlossary('dummy')
        target = RedundantUrlDetector(crawl_job_glossary.redundant_urls_key, persist_interval=32,
                                      host='127.0.0.1', port=DEFAULT_REDIS_PORT)

        url_pattern = "http://www.example.com/path{0}/{1}?param={2}"

        for i in xrange(0, 10):
            for j in xrange(0, 10):
                for k in xrange(0, 10):
                    target.insert_redundant(UrlInfo(url_pattern.format(i, j, k)))

        target = RedundantUrlDetector(crawl_job_glossary.redundant_urls_key, persist_interval=32,
                                      host='127.0.0.1', port=DEFAULT_REDIS_PORT)
        tree = target._get_tree('example.com')
        self.assertEqual(96, tree.count)
