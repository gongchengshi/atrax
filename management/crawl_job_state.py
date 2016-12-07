from aws import USWest2 as AwsConnections
import aws.sdb
from atrax.common.constants import CRAWL_JOB_STATE_DOMAIN_NAME


class CrawlJobState:
    STOPPED = 'stopped'
    PAUSED = 'paused'
    STARTING = 'starting'
    RUNNING = 'running'

    ALL_STATES = [STOPPED, PAUSED, STARTING, RUNNING]

    def __init__(self, crawl_job_name):
        self.crawl_job_name = crawl_job_name
        sdb = AwsConnections.sdb()
        self.crawl_job_state_table = sdb.lookup(CRAWL_JOB_STATE_DOMAIN_NAME)
        if not self.crawl_job_state_table:
            self.crawl_job_state_table = aws.sdb.create_domain(sdb, CRAWL_JOB_STATE_DOMAIN_NAME)

    def get(self):
        item = self.crawl_job_state_table.get_item(self.crawl_job_name)
        return item['state'] if item else CrawlJobState.STOPPED

    def set(self, state):
        if state in CrawlJobState.ALL_STATES:
            self.crawl_job_state_table.put_attributes(self.crawl_job_name, {'state': state})
