import time
from atrax.management.aws_env.fetcher_cluster import FetcherClusterUtil

cluster = FetcherClusterUtil('siemens27012015')

cluster.request_spot_instances('m1.medium', 'us-west-2a', 0.1, 3, time.time() + 60*10)

requests = cluster.get_all_spot_requests()
print len(requests)

requests = cluster.get_unfulfilled_spot_requests()
print len(requests)

requests = cluster.get_open_or_active_spot_requests()
print len(requests)
