from aws import USWest2 as AwsConnections
from boto.s3.key import Key


class CrawledContent:
    def __init__(self, bucket):
        self.bucket = bucket

    def put(self, url_info, contents):
        # This method should not be needed by any clients.
        raise NotImplementedError("put")

    def get(self, key_name):
        k = Key(self.bucket, key_name)
        if not k.exists():
            return None
        return k.get_contents_as_string()


class CrawlJobGlossary:
    def __init__(self, name):
        self._name = name.lower()
        self.crawled_content_bucket_name = 'crawled-content.%s' % self._name
        self.crawled_urls_table_name = 'crawled-urls.%s' % self._name
        self.failed_urls_table_name = 'failed-urls.%s' % self._name
        self.skipped_urls_table_name = 'skipped-urls.%s' % self._name
        self.logs_table_name = 'logs.%s' % self._name
        self.seen_urls_key = self._name + '-seen_urls'
        self.frontier_size_key = self._name + '-frontier_size'


class CrawlJob:
    def __init__(self, name):
        self.name = name
        self.glossary = CrawlJobGlossary(self.name)

        self._sdb = AwsConnections.sdb()
        self._s3 = AwsConnections.s3()

        self.crawled_urls = self._sdb.get_domain(self.glossary.crawled_urls_table_name)
        self.failed_urls = self._sdb.get_domain(self.glossary.failed_urls_table_name)
        self.skipped_urls = self._sdb.lookup(self.glossary.skipped_urls_table_name)
        self.crawled_content_bucket = self._s3.get_bucket(self.glossary.crawled_content_bucket_name)

        self.crawled_content = CrawledContent(self.crawled_content_bucket)

import json
from atrax.common.constants import ORIGINAL_ATTR_NAME
from atrax.common.url_info import UrlInfo
import dateutil.parser
import time


def unpack_sdb_url_info(m):
    u = UrlInfo(m['url'])
    if 'lastCrawled' in m:
        last_crawled = time.mktime(dateutil.parser.parse(m['lastCrawled']).timetuple())
        u.fetched = last_crawled
        u.requested = last_crawled
        u.discovered = last_crawled
    u.referrer_id = m.get('referrer', None)
    if 'robotExclusionReasons' in m:
        u.robot_exclusion_reasons = set(json.loads(m['robotExclusionReasons']) or [])
    if 'googleBotExclusionReasons' in m:
        u.googlebot_exclusion_reasons = set(json.loads(m['googleBotExclusionReasons']) or [])
    u.http_status = int(m.get('httpStatus', None))
    u.redirects_to = m.get('redirectsTo', None)
    # u.fetcher = m.get('crawlerInstance', None)
    # u.process_attempts = m.get('processAttempts', None)
    # u.fingerprint = m.get('fingerprint', None)
    # u.original = m.get(ORIGINAL_ATTR_NAME, None)

    known_attributes = ['url', 'redirectsTo', 'lastCrawled', 'processAttempts', 'crawlerInstance',
                        'httpStatus', 'responseHeaders', 'contentType', 'numRedirects',
                        'googleBotExclusionReasons', 'robotExclusionReasons',
                        'fingerprint', ORIGINAL_ATTR_NAME]

    u.response_headers = {}

    for name, value in m.iteritems():
        if name in known_attributes:
            continue
        u.response_headers[name] = value

    return u
