import re

from aws import USWest2 as AwsConnections
from aws.s3 import get_or_create_bucket, create_bucket
from aws.sdb import get_or_create_domain

from atrax.common.constants import CONTENT_BUCKET_POLICY_FORMAT, DEFAULT_REDIS_PORT
from atrax.common.seen_urls import SeenUrls
from atrax.common.crawled_content import CrawledContent
from atrax.common.exceptions import InvalidCrawlJobNameException
from atrax.management.instance_accessor import InstanceAccessor


class CrawlJobGlossary:
    valid_crawl_job_name_pattern = re.compile(r'[a-z0-9][a-z0-9.-]{2,62}')

    def __init__(self, name):
        if not CrawlJobGlossary.valid_crawl_job_name_pattern.match(name):
            raise InvalidCrawlJobNameException(name)

        self._name = name.lower()
        # S3 Buckets
        self.crawled_content_bucket_name = self._name + '.content'
        self.persisted_frontier_bucket_name = self._name + '.frontier'
        # SimpleDB Domains
        self.crawled_urls_table_name = self._name + '.crawled-urls'
        self.failed_urls_table_name = self._name + '.failed-urls'
        self.skipped_urls_table_name = self._name + '.skipped-urls'
        self.redirected_urls_table_name = self._name + '.redirected-urls'
        self.logs_table_name = self._name + '.logs'
        # Redis Keys
        self.seen_urls_key = self._name + '.seen_urls'
        self.processed_domains_key = self._name + '.processed_domains'
        self.frontier_size_key = self._name + '.frontier_size'
        self.redundant_urls_key = self._name + '.redundant_urls'
        # SQS Queues
        self.unknown_host_queue_name = self._name + '--UnknownHost'

        self.table_names = [self.crawled_urls_table_name, self.failed_urls_table_name,
                            self.skipped_urls_table_name, self.redirected_urls_table_name,
                            self.logs_table_name]

    @property
    def name(self):
        return self._name


class CrawlJob:
    def __init__(self, name, global_config=None):
        self.name = name
        self.config = global_config
        self.glossary = CrawlJobGlossary(self.name)

        self._sdb = AwsConnections.sdb()
        self._s3 = AwsConnections.s3()

        self._logs_table = None
        self._crawled_urls = None
        self._failed_urls = None
        self._skipped_urls = None
        self._redirected_urls = None

        self._persisted_frontier_bucket = None
        self._crawled_content_bucket = None
        self._crawled_content = None

        self._seen_urls = None
        self._instance_accessor = None

    @property
    def logs_table(self):
        if self._logs_table is None:
            self._logs_table = get_or_create_domain(self._sdb, self.glossary.logs_table_name)
        return self._logs_table

    @property
    def crawled_urls(self):
        if self._crawled_urls is None:
            self._crawled_urls = get_or_create_domain(self._sdb, self.glossary.crawled_urls_table_name)
        return self._crawled_urls

    @property
    def failed_urls(self):
        if self._failed_urls is None:
            self._failed_urls = get_or_create_domain(self._sdb, self.glossary.failed_urls_table_name)
        return self._failed_urls

    @property
    def skipped_urls(self):
        if self._skipped_urls is None:
            self._skipped_urls = get_or_create_domain(self._sdb, self.glossary.skipped_urls_table_name)
        return self._skipped_urls

    @property
    def redirected_urls(self):
        if self._redirected_urls is None:
            self._redirected_urls = get_or_create_domain(self._sdb, self.glossary.redirected_urls_table_name)
        return self._redirected_urls

    @property
    def persisted_frontier_bucket(self):
        if self._persisted_frontier_bucket is None:
            self._persisted_frontier_bucket = get_or_create_bucket(self._s3,
                                                                   self.glossary.persisted_frontier_bucket_name)
        return self._persisted_frontier_bucket

    @property
    def crawled_content_bucket(self):
        if self._crawled_content_bucket is None:
            self._crawled_content_bucket = self._s3.lookup(self.glossary.crawled_content_bucket_name)

            if self._crawled_content_bucket is None:
                self._crawled_content_bucket = create_bucket(self._s3, self.glossary.crawled_content_bucket_name)
                bucket_policy = CONTENT_BUCKET_POLICY_FORMAT.format(self.glossary.crawled_content_bucket_name)
                self._crawled_content_bucket.set_policy(bucket_policy)
        return self._crawled_content_bucket

    @property
    def crawled_content(self):
        if self._crawled_content is None:
            self._crawled_content = CrawledContent(self.crawled_content_bucket)
        return self._crawled_content

    @property
    def instance_accessor(self):
        assert(self.config is not None)
        if self._instance_accessor is None:
            self._instance_accessor = InstanceAccessor(self.name, self.config.environment)
        return self._instance_accessor

    @property
    def seen_urls(self):
        if self._seen_urls is None:
            seen_urls_instance = self.instance_accessor.get_redis_instance()
            self._seen_urls = SeenUrls(self.glossary.seen_urls_key,
                                       host=seen_urls_instance.private_ip_address, port=DEFAULT_REDIS_PORT)
        return self._seen_urls
