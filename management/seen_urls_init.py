import sys
from urlparse import urlsplit
from aws import USWest2 as AwsConnections
from atrax.common.crawl_job import CrawlJobGlossary
from atrax.common.seen_urls import SeenUrls
from atrax.common.constants import DEFAULT_REDIS_PORT
from atrax.management.instance_accessor import InstanceAccessorBase


def populate_seen_urls(job_name, environment):
    crawl_job = CrawlJobGlossary(job_name)
    seen_urls_instance = InstanceAccessorBase(job_name, environment).get_redis_instance()
    seen_urls = SeenUrls(crawl_job.seen_urls_key,
                         host=seen_urls_instance.private_ip_address,
                         port=DEFAULT_REDIS_PORT)

    sdb = AwsConnections.sdb()

    for table_name in [crawl_job.failed_urls_table_name,
                       crawl_job.skipped_urls_table_name,
                       crawl_job.crawled_urls_table_name]:
        table = sdb.lookup(table_name)
        items = table.select("select itemName() from `%s`" % table_name)

        for item in items:
            host = urlsplit(item.name)[1]
            last_colon = host.rfind(':')
            domain = host if last_colon == -1 else host[0:last_colon]

            seen_urls.add(item.name, domain)


if __name__ == '__main__':
    populate_seen_urls(sys.argv[1])
