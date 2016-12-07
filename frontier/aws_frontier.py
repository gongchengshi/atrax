import time

from atrax.common.logger import LogType
from atrax.fetcher.fetcher import Fetcher

from atrax.frontier.constants import HOURS_IN_MONTH_F
import atrax.frontier.frontier
from atrax.management.aws_env.constants import FETCHER_INSTANCE_TYPES
from atrax.management.aws_env.fetcher_cluster import FetcherClusterUtil
from aws.ec2 import on_demand_instance_pricing


class AwsFrontier(atrax.frontier.frontier.Frontier):
    def __init__(self, crawl_job_name, logger):
        super(AwsFrontier, self).__init__(crawl_job_name, logger)

        self._frontier_instance = self.crawl_job.instance_accessor.get_frontier_instance()
        # The requests expire after two timer ticks
        self._spot_request_lifetime = self._recurring_timer_interval * 2.0
        self.cluster = FetcherClusterUtil(self.crawl_job.name)

    def _calculate_available_hourly_budget(self):
        return (self._global_config.monthly_budget / HOURS_IN_MONTH_F) - self.cluster.calculate_potential_hourly_cost()

    def run_maintenance(self):
        dead_consumers = self._consumers.find_dead_consumers()
        for consumer in dead_consumers:
            self._consumers.delete(consumer)

        is_scaling = False

        if self._global_config.use_spot_instances:
            # Start more fetcher instances if needed
            instance_type, availability_zone, spot_price = self.cluster.compute_spot_bid(self._frontier_instance.placement)
            num_affordable_instances = int(self._calculate_available_hourly_budget() / spot_price)
            # If there is money in the budget to run at least one more fetcher machines
            self.metrics.put_affordable_instances(num_affordable_instances)
            if num_affordable_instances > 0:
                num_fetchers_needed = self._calculate_num_fetchers_needed()
                num_fetcher_instances_needed = int(num_fetchers_needed / self._global_config.fetchers_per_client)
                num_open_spot_requests = len(self.cluster.get_unfulfilled_spot_requests())
                num_to_request = min(num_affordable_instances, num_fetcher_instances_needed) - num_open_spot_requests
                if num_to_request > 0:
                    self.logger.log(LogType.Info, "Requesting %s fetcher spot instances." % num_to_request)
                    self.cluster.request_spot_instances(instance_type, availability_zone, spot_price, num_to_request,
                                                        time.time() + self._spot_request_lifetime)
                    is_scaling = True
        else:  # Use on demand instances
            price = on_demand_instance_pricing[FETCHER_INSTANCE_TYPES[0]]
            num_affordable_instances = int(self._calculate_available_hourly_budget() / price)
            # If there is money in the budget to run at least one more fetcher machines
            if num_affordable_instances > 0:
                num_fetchers_needed = self._calculate_num_fetchers_needed()
                num_fetcher_instances_needed = int(num_fetchers_needed / self._global_config.fetchers_per_client)

                num_to_request = min(num_affordable_instances, num_fetcher_instances_needed)
                if num_to_request > 0:
                    self.logger.log(LogType.Info, "Requesting %s fetcher on-demand instances." % num_to_request)
                    self.cluster.start_on_demand_instances(num_to_request, self._frontier_instance.placement)
                    is_scaling = True

        self._is_scaling = is_scaling
        self._disperse_unassigned_queues()

    def start_local_fetcher_instance(self, local_id):
        p = Fetcher(self.crawl_job.name, local_id)
        p.start()
