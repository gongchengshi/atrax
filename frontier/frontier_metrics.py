from aws import USWest2 as AwsConnections


class FrontierMetrics:
    def __init__(self, crawl_job_name):
        self.crawl_job_name = crawl_job_name
        self.namespace = 'atrax/' + self.crawl_job_name
        self.cw = AwsConnections.cloudwatch()

    ### Per Fetcher ###
    def put_fetcher_queues(self, fetcher_id, value):
        self.cw.put_metric_data(namespace=self.namespace,
                                name='queues',
                                unit='Count',
                                dimensions={'fetcher_id': fetcher_id},
                                value=value)

    def put_fetcher_active_queues(self, fetcher_id, value):
        self.cw.put_metric_data(namespace=self.namespace,
                                name='active_queues',
                                unit='Count',
                                dimensions={'fetcher_id': fetcher_id},
                                value=value)

    def put_fetcher_per_queue_dequeue_rate(self, fetcher_id, value):
        self.cw.put_metric_data(namespace=self.namespace,
                                name='per_queue_dequeue_rate',
                                unit='Count/Second',
                                dimensions={'fetcher_id': fetcher_id},
                                value=value)

    def put_fetcher_total_dequeue_rate(self, fetcher_id, value):
        self.cw.put_metric_data(namespace=self.namespace,
                                name='total_dequeue_rate',
                                unit='Count/Second',
                                dimensions={'fetcher_id': fetcher_id},
                                value=value)

    ### Frontier ###
    def put_is_scaling(self, value):
        self.cw.put_metric_data(namespace=self.namespace,
                                name='is_scaling',
                                unit='Count',
                                value=1 if value else 0)

    def put_frontier_request_rate(self, value):
        self.cw.put_metric_data(namespace=self.namespace,
                                name='frontier_request_rate',
                                unit='Count/Second',
                                value=value)

    def put_frontier_dequeue_rate(self, value):
        self.cw.put_metric_data(namespace=self.namespace,
                                name='frontier_dequeue_rate',
                                unit='Count/Second',
                                value=value)

    def put_frontier_enqueue_rate(self, value):
        self.cw.put_metric_data(namespace=self.namespace,
                                name='frontier_enqueue_rate',
                                unit='Count/Second',
                                value=value)

    def put_unassigned_queues(self, value):
        self.cw.put_metric_data(namespace=self.namespace,
                                name='unassigned_queues',
                                unit='Count',
                                value=value)

    def put_active_queues(self, value):
        self.cw.put_metric_data(namespace=self.namespace,
                                name='active_queues',
                                unit='Count',
                                value=value)

    def put_available_queues(self, value):
        self.cw.put_metric_data(namespace=self.namespace,
                                name='available_queues',
                                unit='Count',
                                value=value)

    def put_ave_queues_per_consumer(self, value):
        self.cw.put_metric_data(namespace=self.namespace,
                                name='ave_queues_per_consumer',
                                unit='Count',
                                value=value)

    def put_fetchers_needed(self, value):
        self.cw.put_metric_data(namespace=self.namespace,
                                name='fetchers_needed',
                                unit='Count',
                                value=value)

    def put_consumers(self, value):
        self.cw.put_metric_data(namespace=self.namespace,
                                name='consumers',
                                unit='Count',
                                value=value)

    def put_affordable_instances(self, value):
        self.cw.put_metric_data(namespace=self.namespace,
                                name='affordable_instances',
                                unit='Count',
                                value=value)
