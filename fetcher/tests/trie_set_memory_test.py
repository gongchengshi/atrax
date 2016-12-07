import time

from atrax.common.crawl_job import CrawlJob
from python_common.collections.trie_set import TrieSet


crawl_job = CrawlJob('sel11122014')

trie_set = TrieSet()

total_items = 0
for domain in [crawl_job.crawled_urls, crawl_job.skipped_urls, crawl_job.failed_urls, crawl_job.redirected_urls]:
    next_token = None
    query = "select itemName() from `%s`" % domain.name
    while True:
        items = domain.select(query, next_token=next_token)

        num_items = 0
        for item in items:
            trie_set.add(item.name)
            num_items += 1
            total_items += 1

        if items.next_token is None or num_items == 0:
            break
        next_token = next_token

print total_items

while True:
    time.sleep(10)
