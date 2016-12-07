import sys
from atrax.common.crawl_job import CrawlJob
from atrax.management.config_fetcher import ConfigFetcher
from atrax.frontier.remote_frontier_client import get_frontier_client


def new_queue_assigned(queue_name):
    print "Queue assigned: " + queue_name

job_name = sys.argv[1]
config_fetcher = ConfigFetcher(job_name)
config_file = config_fetcher.get_config_file()
global_config = config_file.global_config

crawl_job = CrawlJob(job_name, global_config)
frontier_instance = crawl_job.instance_accessor.get_frontier_instance()
frontier = get_frontier_client(frontier_instance, new_queue_assigned)

# msg_id, url_info = frontier.dequeue('i-5ab7b450:0')
msg_id, url_info = frontier.dequeue('0000:0')

if url_info:
    print "Dequeued: " + url_info.id
else:
    print "Dequeue failed"
