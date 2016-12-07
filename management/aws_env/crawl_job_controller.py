from atrax.management.aws_env.fetcher_cluster import FetcherClusterUtil
from aws import USWest2 as AwsConnections
import aws.s3
import aws.sdb
from atrax.common.crawl_job_notifications import CrawlJobNotifications
from atrax.common.job_config_file import GlobalConfig
from atrax.common.constants import CRAWL_JOB_STATE_DOMAIN_NAME
from atrax.common.crawl_job import CrawlJobGlossary
from atrax.management.config_fetcher import ConfigFetcher
from atrax.management.crawl_job_state import CrawlJobState
from atrax.management.aws_env.frontier_controller import FrontierController


class CrawlJobController:
    def __init__(self, crawl_job_name):
        self.name = crawl_job_name
        self.notifications = CrawlJobNotifications(self.name)
        self.frontier_controller = FrontierController(self.name)
        self.state = CrawlJobState(self.name)
        self._global_config = None

    @property
    def global_config(self):
        if self._global_config is None:
            config_file = ConfigFetcher(self.name).get_config_file()
            self._global_config = GlobalConfig(config_file.colon_delimited_dict('Global'))
        return self._global_config

    def pause(self):
        # terminate fetcher instances
        FetcherClusterUtil(self.name).terminate_fetcher_instances()
        self.frontier_controller.pause()
        self.state.set(CrawlJobState.PAUSED)

    def stop(self):
        self.notifications.stopping_crawl_job()

        self.pause()
        self.frontier_controller.stop()
        self.state.set(CrawlJobState.STOPPED)

    def start(self):
        if self.state.get() in [CrawlJobState.STARTING, CrawlJobState.RUNNING]:
            return

        self.state.set(CrawlJobState.STARTING)

        self.notifications.sync_with_contact_list()

        self.frontier_controller.start('us-west-2a')
        self.state.set(CrawlJobState.RUNNING)

    def destroy(self):
        """
        This terminates all instances and deletes all crawl data and instance storage.
        Deleting the configuration can only happen manually or through Atrax Keeper
        """
        if self.state.get() != CrawlJobState.STOPPED:
            self.notifications.stopping_crawl_job()

        s3 = AwsConnections.s3()
        crawl_job_glossary = CrawlJobGlossary(self.name)
        aws.s3.delete_non_empty_bucket(crawl_job_glossary.crawled_content_bucket_name)

        # Don't call self.stop() because we don't want the frontier controller to attempt to persist the frontier.
        self.pause()  # this terminates the fetchers and stops the frontier

        sdb = AwsConnections.sdb()
        for table_name in crawl_job_glossary.table_names:
            if sdb.lookup(table_name):
                sdb.delete_domain(table_name)

        self.frontier_controller.destroy()

        self.notifications.delete_all_topics()
        crawl_job_state_table = AwsConnections.sdb().get_domain(CRAWL_JOB_STATE_DOMAIN_NAME)
        crawl_job_state_table.delete_attributes(self.name)
