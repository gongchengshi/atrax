from aws import USWest2 as AwsConnections


class FetcherMetrics:
    def __init__(self, crawl_job_name):
        self.crawl_job_name = crawl_job_name
        self.namespace = 'atrax/' + self.crawl_job_name
        self.cw = AwsConnections.cloudwatch()

    def put_already_fetched(self):
        self.cw.put_metric_data(namespace=self.namespace,
                                name='already_fetched',
                                unit='Count',
                                value=1)

    def put_fetched_from_cache(self):
        self.cw.put_metric_data(namespace=self.namespace,
                                name='fetched_from_cache',
                                unit='Count',
                                value=1)

    def put_filtered_from_frontier(self):
        self.cw.put_metric_data(namespace=self.namespace,
                                name='filtered_from_frontier',
                                unit='Count',
                                value=1)

    def put_http_status(self, http_status):
        name = None
        if 200 <= http_status <= 299:
            name = '2xx'
        elif http_status <= 399:
            name = '3xx'
        elif http_status <= 499:
            name = '4xx'
        elif http_status <= 599:
            name = '6xx'
        elif http_status <= 199:
            name = '1xx'
        else:
            raise ValueError('http_status: ' + str(http_status))

        self.cw.put_metric_data(namespace=self.namespace,
                                name=name,
                                unit='Count',
                                value=1)
