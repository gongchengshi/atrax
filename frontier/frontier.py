from collections import deque
import time
from threading import Timer
from abc import abstractmethod, ABCMeta

import zmq
from boto.sqs.message import RawMessage as SqsMessage
from python_common.collections.utils import rotate_to_first_in_deque_and_pop, count_if
from aws import USWest2 as AwsConnections
from atrax.common.logger import LogType
from atrax.common.fetcher_id import create_fetcher_id
from atrax.common.constants import LOCALHOST_IP
from atrax.common.crawl_job import CrawlJob
from atrax.management.config_fetcher import ConfigFetcher
from atrax.frontier.frontier_queue import FrontierQueue
from atrax.frontier.utils import build_queue_name
from atrax.frontier.constants import *
from atrax.frontier.frontier_metrics import FrontierMetrics
from atrax.frontier.consumer_collection import ConsumerCollection
from atrax.frontier.exceptions import FrontierEmpty


class Frontier:
    __metaclass__ = ABCMeta
    DEQUEUE_INTERVAL_MARGIN = 1.10  # 10% margin

    def __init__(self, crawl_job_name, logger):
        self.logger = logger
        config_fetcher = ConfigFetcher(crawl_job_name)
        config_file = config_fetcher.get_config_file()
        self._global_config = config_file.global_config
        self.crawl_job = CrawlJob(crawl_job_name, self._global_config)

        self._recurring_timer_interval = self._global_config.lb_maintenance_cycle_period * SECONDS_PER_MINUTE
        self.metrics = FrontierMetrics(self.crawl_job.name)

        local_fetcher_id = create_fetcher_id(self._global_config.environment, 0)

        # The minimum dequeue interval that every consumer must have in order to be considered as a queue donor.
        min_dequeue_interval = (1.0 / self._global_config.max_fetch_rate) * Frontier.DEQUEUE_INTERVAL_MARGIN
        self._consumers = ConsumerCollection(local_fetcher_id, min_dequeue_interval, self.crawl_job.instance_accessor,
                                             self._recurring_timer_interval, self.logger)

        self._queues_by_name = {}
        self._unassigned_queues = deque()
        for queue in AwsConnections.sqs().get_all_queues(self.crawl_job.name):
            frontier_queue = FrontierQueue(queue)
            self._queues_by_name[queue.name] = frontier_queue
            if frontier_queue.count > 0:
                self._unassigned_queues.appendleft(frontier_queue)
            else:
                self._unassigned_queues.append(frontier_queue)

        self._is_scaling = True

        # This is a hack to serialize the execution of asynchronous operations into the main thread.
        self._zmq_context = zmq.Context.instance()
        self._zmq_socket = self._zmq_context.socket(zmq.REQ)
        self._zmq_socket.connect('tcp://%s:%s' % (LOCALHOST_IP, str(DEFAULT_FRONTIER_PORT)))

        self._enqueue_count = 0
        self._dequeue_count = 0
        self._previous_emit_time = time.time()

    def start(self):
        self.start_local_fetcher_instance(0)

        # The first maintenance run should happen within 1 minute of the frontier starting.
        Timer(SECONDS_PER_MINUTE, self._recurring_timer_event).start()
        Timer(SECONDS_PER_MINUTE, self._emit_metrics_event).start()

    @abstractmethod
    def start_local_fetcher_instance(self, local_id):
        pass

    def _emit_metrics_event(self):
        for consumer in self._consumers:
            self.metrics.put_fetcher_queues(consumer.fetcher_id, consumer.queue_count)
            self.metrics.put_fetcher_per_queue_dequeue_rate(consumer.fetcher_id, consumer.per_queue_dequeue_rate)
            self.metrics.put_fetcher_active_queues(consumer.fetcher_id, consumer.count_active_queues())
            self.metrics.put_fetcher_total_dequeue_rate(consumer.fetcher_id, consumer.consumer_dequeue_rate)

        self.metrics.put_unassigned_queues(len(self._unassigned_queues))
        self.metrics.put_active_queues(count_if(self._queues_by_name.values(), lambda q: q.active))
        self.metrics.put_is_scaling(self._is_scaling)
        self.metrics.put_consumers(len(self._consumers))

        self.logger.log(LogType.Debug, "Calling emit_metrics")
        self._zmq_socket.send_json({'cmd': 'emit_metrics'})
        self._zmq_socket.recv()
        Timer(SECONDS_PER_MINUTE, self._emit_metrics_event).start()

    def emit_metrics(self):
        now = time.time()
        self.metrics.put_frontier_dequeue_rate(self._dequeue_count / (now - self._previous_emit_time))
        self.metrics.put_frontier_enqueue_rate(self._enqueue_count / (now - self._previous_emit_time))
        self._enqueue_count = 0
        self._dequeue_count = 0
        self._previous_emit_time = now

    ### Maintenance thread ###
    def _recurring_timer_event(self):
        self._zmq_socket.send_json({'cmd': 'run_maintenance'})
        self._zmq_socket.recv()
        Timer(self._recurring_timer_interval, self._recurring_timer_event).start()

    @abstractmethod
    def run_maintenance(self):
        pass

    def _count_num_available_queues(self):
        num_available_queues = count_if(self._unassigned_queues, lambda q: q.active)
        num_available_queues += count_if(self._consumers, lambda c: c.count_donatable_queues())
        return num_available_queues

    def _calculate_num_fetchers_needed(self):
        num_available_queues = self._count_num_available_queues()
        ave_queues_per_consumer = self._consumers.calculate_ave_active_queues()
        num_fetchers_needed = (num_available_queues / ave_queues_per_consumer) if ave_queues_per_consumer > 0 else 0
        self.metrics.put_available_queues(num_available_queues)
        self.metrics.put_ave_queues_per_consumer(ave_queues_per_consumer)
        self.metrics.put_fetchers_needed(num_fetchers_needed)
        return num_fetchers_needed

    ### Dequeue ###
    def dequeue(self, fetcher_id):
        self._dequeue_count += 1
        consumer = self._consumers[fetcher_id]
        if consumer.stop_requested:
            self._delete_consumer(consumer)  # The consumer is being stopped. We won't be hearing from it again.
            # Todo: Return a special value here to tell the fetcher that it is intentionally being stopped?
            # This would allow the fetcher to log the reason it is being stopped. Do this anywhere None is
            # being returned in order to stop the fetcher intentionally.
            return None

        if consumer.available_time > 0 or not consumer.has_queues:  # The consumer is underloaded so give it more work
            if len(self._consumers) > 1:
                self._load_consumer(consumer)
            elif len(self._consumers) == 1:
                if len(self._unassigned_queues) == 0:
                    time.sleep(consumer.available_time)
                else:
                    self._load_consumer(consumer)

        item = consumer.dequeue()
        if item is None:
            self._load_consumer(consumer)
            item = consumer.dequeue()
            if item is None:
                self.logger.log(LogType.Info, "Stopping " + consumer.fetcher_id)
                self._delete_consumer(consumer)  # The consumer is being stopped. We won't be hearing from it again.
                if len(self._consumers) == 0:  # Nothing left to crawl
                    raise FrontierEmpty()
        return item

    def _load_consumer(self, consumer):
        """
        Assign the first available queue to the consumer and return immediately. If no queue can be assigned to
        the consumer then the youngest consumer is scheduled to stop and one of it's queues is donated to the consumer.
        """
        # Find an active queue in the unassigned queues list
        queue = rotate_to_first_in_deque_and_pop(self._unassigned_queues, lambda q: q.active)
        if queue:
            consumer.assign_queue(queue)
            return True

        # No active queue could be found so assign the first queue with estimated count > 0
        queue = rotate_to_first_in_deque_and_pop(self._unassigned_queues, lambda q: q.count > 0)
        if queue:
            consumer.assign_queue(queue)
            return True

        donor = self._consumers.get_donor(consumer)
        if donor:
            # Give the consumer the donor's newest active queue
            queue = donor.remove_youngest_active_queue()
            consumer.assign_queue(queue)
            self.logger.log(LogType.Debug, "%s donated %s to %s" % (donor.fetcher_id, queue.name, consumer.fetcher_id))
            return True

        # If we get here then no queue was found to give to the consumer. A consumer can be shutdown.
        youngest_consumer = self._consumers.get_least_senior()
        if youngest_consumer:
            self.logger.log(LogType.Info, "Stopping youngest consumer: " + youngest_consumer.fetcher_id)
            youngest_consumer.request_stop()
            self._recycle_queues(youngest_consumer.queues)
            youngest_consumer.reset()
            if consumer != youngest_consumer and len(self._unassigned_queues) > 0:
                consumer.assign_queue(self._unassigned_queues.popleft())
                return True
        return False

    def _disperse_unassigned_queues(self):
        if not self._is_scaling:
            for consumer in self._consumers:
                if consumer.available_time > 0:
                    self._is_scaling = True
                    return

        if not self._is_scaling:
            try:
                while len(self._unassigned_queues) > 0:
                    for consumer in self._consumers:
                        consumer.assign_queue(self._unassigned_queues.popleft())
            except IndexError:
                pass

    def _delete_consumer(self, consumer):
        self._recycle_queues(consumer.queues)
        self._consumers.delete(consumer)

    ### Enqueue ###
    def enqueue(self, queue_key, url_key, frontier_message):
        self._dequeue_count += 1
        queue = self._get_frontier_queue(queue_key)
        queue.enqueue(frontier_message)
        self.crawl_job.seen_urls.add(url_key)

    def enqueue_batch(self, queue_key, url_tuples):
        self._dequeue_count += len(url_tuples)
        queue = self._get_frontier_queue(queue_key)

        batch = []
        url_ids = []
        count = 0
        for url_id, msg in url_tuples:
            batch.append(msg)
            url_ids.append(url_id)
            count += 1
            if count == SQS_MAX_BATCH_SIZE:
                queue.enqueue_batch(batch)
                for u in url_ids:
                    self.crawl_job.seen_urls.add(u)
                batch = []
                url_ids = []
                count = 0

        if count > 0:
            queue.enqueue_batch(batch)
            for u in url_ids:
                self.crawl_job.seen_urls.add(u)

    ### queue management ###
    def _get_frontier_queue(self, queue_key):
        queue_name = build_queue_name(self.crawl_job.name, queue_key)

        try:
            queue = self._queues_by_name[queue_name]
        except KeyError:  # This should never happen
            queue = FrontierQueue(queue_name)
            self._add_to_unassigned_queues(queue)
            self._queues_by_name[queue_name] = queue

        return queue

    def _add_to_unassigned_queues(self, queue):
        # Insert queue at the front of the deque if it is active otherwise append it to the back
        if queue.active:
            self._unassigned_queues.appendleft(queue)
        else:
            self._unassigned_queues.append(queue)

    def _recycle_queues(self, queues):
        for queue in queues:
            self._add_to_unassigned_queues(queue)

    ### Additional public methods ###
    def delete(self, queue_name, msg_id):
        """
        This method is no longer used because frontier clients now handle deleting messages.
        This is only here to complete the interface.
        """
        m = SqsMessage()
        m.receipt_handle = msg_id
        m.queue = self._queues_by_name[queue_name].queue
        m.delete()

    def dequeue_failed(self, queue_name):
        self._queues_by_name[queue_name].dequeue_failed()

    def delete_queue(self, queue_name):
        """
        It is only necessary to call this method if a SQS queue was removed by something other than the frontier.
        This should never happen.
        """
        queue = self._queues_by_name[queue_name]

        for consumer in self._consumers:
            consumer.remove_queue(queue)

        for i in xrange(0, len(self._unassigned_queues)):
            q = self._unassigned_queues.popleft()
            if q == queue:
                continue  # use continue instead of break to maintain original deque order
            self._unassigned_queues.append(q)

        del self._queues_by_name[queue_name]
