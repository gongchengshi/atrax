from collections import deque
import time
import math

from python_common.collections.utils import count_if
from atrax.common.logger import LogType


class FrontierConsumer:
    def __init__(self, fetcher_id, min_dequeue_interval, logger):
        self.logger = logger
        self.fetcher_id = fetcher_id
        self.date_created = time.time()
        self.stop_requested = False

        self._min_dequeue_interval = min_dequeue_interval
        self._cycle_interval = 0.0  # How often each queue being used
        self._previous_cycle_length = 0
        self._current_cycle_length = 0
        self._cycle_marker = object()
        self._queues = deque()
        self._queues.append(self._cycle_marker)
        self._last_dequeue_attempt = self.date_created
        # The last time a dequeue attempt was made on the first queue in the queue list.
        self._last_cycle_time = self._last_dequeue_attempt
        self._dequeue_times = deque([self._last_dequeue_attempt, self._last_dequeue_attempt], maxlen=2)
        self._total_diff_between_dequeues = 0
        self._dequeue_count = 0
        self._queue_history = set()

    # ## Queue collection operations ###
    @property
    def queues(self):
        return [q for q in self._queues if q != self._cycle_marker]

    def assign_queue(self, queue):
        queue.time_assigned = time.time()
        self._queues.append(queue)
        self._queue_history.add(hash(queue.name))

    def reset(self):
        self._queues = deque()
        self._queues.append(self._cycle_marker)
        self._cycle_interval = 0.0
        self._previous_cycle_length = 0
        self._current_cycle_length = 0

    @property
    def has_queues(self):
        return self.queue_count > 0

    @property
    def queue_count(self):
        return len(self._queues) - 1

    def count_active_queues(self):
        return count_if(self._queues, lambda q: q != self._cycle_marker and q.active)

    def remove_queue(self, queue):
        self._queues = deque([q for q in self._queues if q != queue])

    def remove_youngest_active_queue(self):
        found_queue = None
        for queue in sorted(self._queues,
                            key=lambda q: (0 if q == self._cycle_marker else q.time_assigned), reverse=True):
            if queue == self._cycle_marker:
                continue
            if queue.active:
                found_queue = queue
                break

        if found_queue:
            self.remove_queue(found_queue)
        return found_queue

    @property
    def seniority(self):
        return math.log(self._dequeue_count) * len(self._queue_history)

    def dequeue(self):
        self._last_dequeue_attempt = time.time()

        if self.stop_requested:
            return None

        if not self.has_queues:
            self.logger.log(LogType.Debug, "%s has no queues" % self.fetcher_id)
            return None

        msg = None
        for _ in xrange(0, len(self._queues)):
            queue = self._queues.popleft()
            self._queues.append(queue)
            if queue == self._cycle_marker:
                self._cycle_interval = self._last_dequeue_attempt - self._last_cycle_time
                self._last_cycle_time = self._last_dequeue_attempt
                self._previous_cycle_length = self._current_cycle_length
                self._current_cycle_length = 0
                continue

            self._current_cycle_length += 1
            msg = queue.dequeue()
            if msg is not None:
                break

        if msg is None:
            # check to see if all queues really are inactive by dequeuing directly from them
            for queue in self._queues:
                if queue == self._cycle_marker:
                    continue
                msg = queue.dequeue(direct=True)
                if msg is not None:
                    break

        if msg is not None:
            self._dequeue_times.append(self._last_dequeue_attempt)
            self._total_diff_between_dequeues += self._dequeue_times[1] - self._dequeue_times[0]
            self._dequeue_count += 1
        else:
            self.logger.log(LogType.Debug, "All queues assigned to %s are inactive" % self.fetcher_id)
        return msg

    @property
    def per_queue_dequeue_rate(self):
        """
        :return: The number of dequeues per second per queue.
        """
        return (1.0 / self._cycle_interval) if self._cycle_interval else 0.0

    @property
    def consumer_dequeue_rate(self):
        """
        :return: The mean rate of dequeue requests by this consumer since it was created
        """
        return (self._total_diff_between_dequeues / self._dequeue_count) if self._dequeue_count > 0 else 0.0

    def count_donatable_queues(self):
        if self._previous_cycle_length == 0 or self._cycle_interval == 0:
            return 0

        sec_between_dequeue = self._cycle_interval / self._previous_cycle_length
        # The +1 accounts for precision truncation and allows the consumer to be more greedy than simply rounding up.
        num_needed_queues = int(self._min_dequeue_interval / sec_between_dequeue) + 1

        num_active_queues = self.count_active_queues()
        if num_active_queues == 0 or num_active_queues < num_active_queues:
            return 0

        return num_active_queues - num_needed_queues

    @property
    def available_time(self):
        return self._min_dequeue_interval - self._cycle_interval

    @property
    def seconds_since_last_dequeue(self):
        return time.time() - self._last_dequeue_attempt

    def request_stop(self):
        self.stop_requested = True
