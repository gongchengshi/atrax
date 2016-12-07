import multiprocessing
import time
from atrax.fetcher.fetcher import Fetcher
from atrax.frontier.frontier import Frontier
import random
import atrax.management.local_env.instance_accessor


class LocalFrontier(Frontier):
    def __init__(self, crawl_job_name, logger):
        super(LocalFrontier, self).__init__(crawl_job_name, logger)

    def run_maintenance(self):
        dead_consumers = self._consumers.find_dead_consumers()
        for consumer in dead_consumers:
            self._consumers.delete(consumer)

        """
        Start more fetcher instances if needed
        """
        is_scaling = False

        num_fetchers_needed = self._calculate_num_fetchers_needed()
        max_fetchers = multiprocessing.cpu_count()
        num_to_request = max_fetchers - len(self.crawl_job._instance_accessor.get_fetcher_instances()) - num_fetchers_needed

        for _ in xrange(0, num_to_request):
            self.start_local_fetcher_instance(random.randint(1, 100))
            is_scaling = True
            time.sleep(1)

        self._is_scaling = is_scaling
        self._disperse_unassigned_queues()

    def start_local_fetcher_instance(self, local_id):
        instance_id = 0
        for p in atrax.management.local_env.instance_accessor.LocalInstanceAccessor.processes:
            if p.is_alive():
                instance_id += 1
            else:
                break

        # All local fetcher instances have a de facto id of '0000' when they appear in the logs.
        # In order to discern between them in the logs, pass the instance id as the local id.
        # p = FetcherProcess(self.crawl_job_name, instance_id)
        p = Fetcher(self.crawl_job.name, instance_id)
        atrax.management.local_env.instance_accessor.LocalInstanceAccessor.processes.append(p)
        p.start()
        pass
