import os
from atrax.common.constants import REASON_ATTR_NAME, REFERRER_ID_ATTR_NAME
from atrax.common.crawl_job import CrawlJob
from atrax.common.crawl_scope import CrawlerScope, UrlClass
from atrax.common.url_info import UrlInfo
from atrax.management.aws_env.constants import LOCAL_CRAWL_JOB_DIR
from atrax.management.config_fetcher import ConfigFetcher


def purge_out_of_scope(self, crawl_job_name):
    """
    Go through crawled_urls and move them into skipped_urls and delete the content from s3.
    """
    # Todo: Not tested
    scope = CrawlerScope(ConfigFetcher(crawl_job_name).get_scope_file())
    crawl_job = CrawlJob(crawl_job_name)

    next_token_file_path = os.path.join(LOCAL_CRAWL_JOB_DIR, self.name,
                                        "purge_out_of_scope_next_crawled_urls_token.txt")

    with open(next_token_file_path, 'r') as next_token_file:
        prev_next_token = next_token_file.read()

    query = "select `url`, `referrer_id` from `{0}`".format(crawl_job.glossary.crawled_urls_table_name)
    try:
        items = crawl_job.crawled_urls.select(query, next_token=prev_next_token)
    except:
        items = crawl_job.crawled_urls.select(query)

    next_token = items.next_token

    count = 0
    try:
        for item in items:
            count += 1

            if prev_next_token != items.next_token:
                prev_next_token = next_token
                next_token = items.next_token

            url_info = UrlInfo(item['url'], canonized_url=item['url'])
            c = scope.get(url_info)
            if c == UrlClass.InScope or item.name.endswith('robots.txt') or item.name.endswith('sitemap.xml'):
                continue

            attributes = {REASON_ATTR_NAME: c}
            referrer_id = item.get(REFERRER_ID_ATTR_NAME, None)
            if referrer_id:
                attributes[REFERRER_ID_ATTR_NAME] = referrer_id
            crawl_job.skipped_urls.put_attributes(item.name, attributes)
            key = crawl_job.crawled_content_bucket.get_key(url_info.s3_key)

            if key:
                key.delete()
            item.delete()  # Todo: do this in batches?
    except Exception, ex:
        with open(next_token_file_path, 'w') as next_token_file:
            next_token_file.write(prev_next_token)
        print "Interrupted after %s records." % count
        raise

    os.remove(next_token_file_path)

    # Todo: Go through redirected_urls and failed_urls tables and move the out-of-scope entries to skipped_urls
    # Todo: The documents should also be removed from Cleaver but this can be managed from Atrax Keeper.
    # Todo: Atrax Keeper should also remove the documents from Cleaver that were extracted from archive files
