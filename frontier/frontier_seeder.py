import os
import time
import sys

from boto.exception import S3ResponseError

from atrax.common.crawl_scope import CrawlerScope, UrlClass
from atrax.common.logger import LogType, SimpleLogger
from atrax.common.sdb_url_info import unpack_sdb_url_info
from atrax.common.url_info import UrlInfo
from atrax.fetcher.fetcher import Fetcher
from atrax.frontier.message import pack_message
from atrax.frontier.utils import QueueKeyDict
from atrax.management.config_fetcher import ConfigFetcher
from atrax.management.aws_env.frontier_controller import FrontierController
from atrax.management.aws_env.constants import LOCAL_CRAWL_JOB_DIR
from atrax.prior_versions import crawl_job_versions, unpack_sdb_url_info_versions
from python_common.web.url_utils import InvalidUrl


class FrontierSeeder:
    def __init__(self, config, frontier):
        self.config = config
        self.config_fetcher = ConfigFetcher(self.config.job_name)
        self.frontier_controller = FrontierController(self.config.job_name)
        self.frontier = frontier
        self.queue_names = QueueKeyDict()
        self.logger = SimpleLogger(self.frontier.logger.log_table,
                                   self.frontier.logger.source.replace('frontier', 'seeder'))

    def run(self):
        self.logger.log(LogType.Info, "Started")
        try:
            self.seed_from_file()
        except S3ResponseError, ex:
            if ex.status != 404:
                raise
        if self.config.seed_from_reference_job:
            self.enqueue_seeds_from_crawl_job()
        self.logger.log(LogType.Info, "Finished")

    def seed_from_file(self):
        seeds_file_path = os.path.join(LOCAL_CRAWL_JOB_DIR, self.config.job_name, self.config.job_name + '.seeds')
        curr_modified = None
        new_modified = None

        if os.path.exists(seeds_file_path):
            curr_modified = os.path.getmtime(seeds_file_path)

        self.config_fetcher.get_seeds_file()

        if curr_modified and os.path.exists(seeds_file_path):
            new_modified = os.path.getmtime(seeds_file_path)

        if (not curr_modified) or (new_modified and curr_modified < new_modified):
            try:
                self.enqueue_seeds_from_file(seeds_file_path)
            except Exception, ex:
                os.remove(seeds_file_path)
                raise

    def enqueue(self, url_info):
        self.frontier.enqueue(self.queue_names[url_info], url_info.id, pack_message(url_info))

    def enqueue_seeds_from_file(self, file_path):
        with open(file_path, 'r') as urls_file:
            for line in urls_file:
                url_info = UrlInfo(line.strip())
                url_info.is_seed = True
                url_info.discovered = time.time()
                self.enqueue(url_info)

    def enqueue_seeds_from_crawl_job(self):
        fetcher = Fetcher(self.config.job_name, 'seeder')

        if fetcher.reference_job is None:
            return

        if self.config.reference_job_version < self.config.version:
            _unpack_sdb_url_info = unpack_sdb_url_info_versions[self.config.reference_job_version]
        else:
            _unpack_sdb_url_info = unpack_sdb_url_info

        crawl_job_path = os.path.join(LOCAL_CRAWL_JOB_DIR, self.config.job_name)
        next_token_file_path = os.path.join(crawl_job_path, fetcher.reference_job.name + "_next_crawled_urls_token.txt")

        if not os.path.exists(crawl_job_path):
            os.makedirs(crawl_job_path)

        if os.path.exists(next_token_file_path):
            with open(next_token_file_path, 'r') as next_token_file:
                next_token = next_token_file.read()
                if not next_token:
                    return  # All URLs from the crawl job have already been processed
        else:
            next_token = None

        query = "select * from `{0}` where `redirectsTo` is null".format(
            fetcher.reference_job.glossary.crawled_urls_table_name)
        items = fetcher.reference_job.crawled_urls.select(query, next_token=next_token)

        count = 0
        try:
            for item in items:
                count += 1
                if count & 1023 == 0:  # save the last complete token every 1024 items
                    with open(next_token_file_path, 'w') as next_token_file:
                        next_token_file.write(items.next_token)

                try:
                    url_info = _unpack_sdb_url_info(item)
                    fetcher.url_transformer.transform(url_info)

                    # Fetchers are running in parallel. This avoids adding duplicate URLs
                    if (url_info.id, url_info.domain) in self.frontier.crawl_job.seen_urls:
                        continue

                    fetcher.process_domain(url_info.domain)
                    fetcher.set_exclusion_reasons(url_info)

                    if not fetcher.robot_is_excluded(url_info) and fetcher.scope.get(url_info) == UrlClass.InScope:
                        url_info.is_seed = True
                        if not url_info.discovered:
                            url_info.discovered = time.time()
                        self.enqueue(url_info)
                except InvalidUrl, ex:
                    self.logger.log(LogType.InternalWarning, "Failed to add seed", item.name, ex, sys.exc_info())
        except Exception as ex:
            self.logger.log(LogType.InternalWarning, "Interrupted", None, ex, sys.exc_info())
            raise
        finally:
            if items.next_token:
                with open(next_token_file_path, 'w') as next_token_file:
                    next_token_file.write(items.next_token)

            self.logger.log(LogType.Info, "Processed %s URLs" % count)

        with open(next_token_file_path, 'w') as next_token_file:
            next_token_file.truncate()  # Signifies that all of the URLs have been processed


def export_seeds_from_crawl_job(output_path, dest_crawl_job_name, src_crawl_job_name, version):
    crawl_job = crawl_job_versions[version](src_crawl_job_name)
    scope = CrawlerScope(ConfigFetcher(dest_crawl_job_name).get_scope_file())
    query = "select `url` from `{0}` where `url` is not null and `redirectsTo` is null".format(
        crawl_job.crawled_urls.name)

    with open(output_path, 'w') as output_file:
        items = crawl_job.crawled_urls.select(query)

        count = 0
        try:
            for item in items:
                url = item['url']
                count += 1
                if scope.get(UrlInfo(url)) == UrlClass.InScope:
                    output_file.write(url + '\n')
        except Exception as ex:
            print "Interrupted after %s records" % count
            raise


class FrontierSeederClient(FrontierSeeder):
    def __init__(self, config, frontier):
        FrontierSeeder.__init__(self, config, frontier)

    def enqueue(self, url_info):
        self.frontier.enqueue([url_info])
