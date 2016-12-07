from threading import Timer

from atrax.common.crawl_job import CrawlJob
from aws import USWest2 as AwsConnections
from aws.sdb import count
from atrax.management.config_fetcher import ConfigFetcher
from atrax.frontier.constants import SECONDS_PER_MINUTE



class MetricsService():
    def __init__(self, crawl_job_name, emit_interval):
        """
        :param crawl_job_name:
        :param emit_interval: How often to emit the metrics in minutes.
        """
        config_fetcher = ConfigFetcher(crawl_job_name)
        config_file = config_fetcher.get_config_file()
        self._global_config = config_file.global_config
        self.crawl_job = CrawlJob(crawl_job_name, self._global_config)
        self.emit_interval = emit_interval * SECONDS_PER_MINUTE
        self.namespace = 'atrax/' + self.crawl_job.name
        self.cw = AwsConnections.cloudwatch()
        self.sqs = AwsConnections.sqs()

    def start(self):
        self._emit_metrics_event()

    def _emit_metrics_event(self):
        self._emit_frontier_metrics()
        self._emit_crawler_metrics()
        Timer(self.emit_interval, self._emit_metrics_event).start()

    def _emit_crawler_metrics(self):
        self.cw.put_metric_data(namespace=self.namespace,
                                name='crawled_urls',
                                unit='Count',
                                value=count(self.crawl_job.crawled_urls))

        self.cw.put_metric_data(namespace=self.namespace,
                                name='machine_count',
                                unit='Count',
                                value=len(self.crawl_job.instance_accessor.get_crawl_job_instances()))

    def _emit_frontier_metrics(self):
        total_messages = 0
        total_queues = 0
        num_nonempty_queues = 0
        # Todo: Speed this up by doing something similar to the backing off algorithm in FrontierQueue.
        for queue in self.sqs.get_all_queues(self.crawl_job.name):
            queue_message_count = queue.count()
            if queue_message_count > 0:
                num_nonempty_queues += 1
            total_messages += queue_message_count
            total_queues += 1

        self.cw.put_metric_data(namespace=self.namespace,
                                name='frontier_size',
                                unit='Count',
                                value=total_messages)

        self.cw.put_metric_data(namespace=self.namespace,
                                name='total_queues',
                                unit='Count',
                                value=total_queues)

        self.cw.put_metric_data(namespace=self.namespace,
                                name='num_nonempty_queues',
                                unit='Count',
                                value=num_nonempty_queues)
