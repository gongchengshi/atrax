from collections import defaultdict
from atrax.common.fetcher_id import parse_id
from atrax.frontier.frontier_consumer import FrontierConsumer
from atrax.management.constants import InstanceState


class ConsumerCollection:
    def __init__(self, first_fetcher_id, min_dequeue_interval, instance_accessor, timeout, logger):
        self._min_dequeue_interval = min_dequeue_interval
        self._instance_accessor = instance_accessor
        self._timeout = timeout
        self._logger = logger

        self._consumers = {}
        self._consumers_by_instance_id = defaultdict(set)
        self.first_consumer = self[first_fetcher_id]  # this consumer is safe from removal

    def find_dead_consumers(self):
        dead_consumers = []

        for instance_id, consumers in self._consumers_by_instance_id.iteritems():
            all_timed_out = True
            for consumer in consumers:
                all_timed_out &= (consumer.seconds_since_last_dequeue > self._timeout)
            if all_timed_out:
                fetcher_instance = self._instance_accessor.get_instance(instance_id)
                if fetcher_instance is None or fetcher_instance.state not in [InstanceState.PENDING,
                                                                              InstanceState.RUNNING]:
                    # Todo: if fetcher_instance is in alarmed state: terminate the instance
                    dead_consumers.extend(consumers)

        return dead_consumers

    def get_least_senior(self):
        for consumer in sorted(self._consumers.values(), key=lambda c: c.senority):
            if consumer != self.first_consumer:
                return consumer
        return None

    def get_donor(self, recipient):
        """
        :param recipient: The consumer that will receive the queue
        :return: A consumer that is most able to donate
        """
        prospective_donors = []
        for prospective_donor in self._consumers.values():
            num_donatable_queues = prospective_donor.count_donatable_queues()
            if num_donatable_queues > 0 and prospective_donor != recipient:
                prospective_donors.append((prospective_donor, num_donatable_queues))

        if len(prospective_donors) > 0:
            return max(prospective_donors, key=lambda p: p[1])[0]
        return None

    def calculate_ave_active_queues(self):
        if len(self._consumers) == 0:
            return 0
        return sum([c.count_active_queues() for c in self._consumers.values()]) / len(self._consumers)

    def __getitem__(self, fetcher_id):
        try:
            consumer = self._consumers[fetcher_id]
        except KeyError:
            consumer = FrontierConsumer(fetcher_id, self._min_dequeue_interval, self._logger)
            instance_id, local_id = parse_id(fetcher_id)
            self._consumers_by_instance_id[instance_id].add(consumer)
            self._consumers[fetcher_id] = consumer
        return consumer

    def delete(self, consumer):
        del self._consumers[consumer.fetcher_id]
        instance_id, local_id = parse_id(consumer.fetcher_id)
        self._consumers_by_instance_id[instance_id].remove(consumer)
        if len(self._consumers_by_instance_id[instance_id]) == 0:
            del self._consumers_by_instance_id[instance_id]

    def __len__(self):
        return len(self._consumers)

    def __iter__(self):
        return self._consumers.itervalues()
