from collections import deque
from boto.sqs.queue import Queue as SqsQueue
from boto.sqs.message import RawMessage as SqsMessage
from boto.exception import SQSError
import time

from aws import USWest2 as AwsConnections
from aws.exceptions import NonExistantSqsQueueException, AwsErrorCodes


class FrontierQueue:
    def __init__(self, queue_or_queue_name):
        if isinstance(queue_or_queue_name, SqsQueue):
            self._queue = queue_or_queue_name
        else:  # it must be a queue name
            self._queue = FrontierQueue.create_sqs_queue(queue_or_queue_name)
        self._queue.set_message_class(SqsMessage)
        self._skip_count = 0
        self._empty_count = 0
        self.time_assigned = 0

        # Used to calculate dequeue rate
        self._total_diff_between_dequeues = 0
        self._dequeue_times = deque([0, 0], maxlen=2)
        self._dequeue_count = 0

    @property
    def name(self):
        return self._queue.name

    @property
    def dequeue_rate(self):
        return self._total_diff_between_dequeues / self._dequeue_count

    @property
    def count(self):
        """
        This has the side effect of deactivating queues with count == 0.
        Warning: This method makes an AWS API call and can take up to .1 seconds to return.
        """
        c = self._queue.count()
        if c > 1:  # Allow a fudge factor of 1 before reactivating a queue
            self._activate()
        elif c == 0 and self._presume_active:
            self._skip_count += 1
        return c

    @property
    def active(self):
        return self._empty_count == 0 and self._skip_count == 0

    @property
    def _presume_active(self):
        return self._empty_count >= self._skip_count

    def _activate(self):
        self._empty_count = 0
        self._skip_count = 0

    def dequeue_failed(self):
        if self._presume_active:
            self._empty_count = 0
            self._skip_count += 1
        else:
            self._empty_count += 1

    def dequeue(self, direct=False):
        try:
            if direct:
                msg = self._queue.read()
                if msg is None:
                    self.dequeue_failed()
                else:
                    self._activate()
                return msg

            return self.name if self._presume_active else None
        except SQSError, ex:
            if ex.error_code == AwsErrorCodes.SqsNonExistentQueue:
                raise NonExistantSqsQueueException(self.name)
            else:
                raise

    def update_dequeue_time(self, timestamp=None):
        self._dequeue_times.append(timestamp or time.time())
        self._total_diff_between_dequeues += self._dequeue_times[1] - self._dequeue_times[0]

    def enqueue(self, msg):
        try:
            message = SqsMessage(body=msg)
            self._queue.write(message)
            self._activate()
        except SQSError, ex:
            if ex.error_code == AwsErrorCodes.SqsNonExistentQueue:
                raise NonExistantSqsQueueException(self.name)
            else:
                raise

    def enqueue_batch(self, msgs):
        try:
            batch = []
            index = 0
            for msg in msgs:
                batch.append((index, msg, 0))
                index += 1

            self._queue.write_batch(batch)
            self._activate()
        except SQSError, ex:
            if ex.error_code == AwsErrorCodes.SqsNonExistentQueue:
                raise NonExistantSqsQueueException(self.name)
            else:
                raise

    @staticmethod
    def create_sqs_queue(queue_name):
        queue = AwsConnections.sqs().create_queue(queue_name, 10 * 60)  # 10 minutes
        queue.set_attribute('MessageRetentionPeriod', 1209600)  # 14 days
        # Defaults:
        # queue.set_attribute('DelaySeconds', 0)  # Don't delay
        # queue.set_attribute('MaximumMessageSize', 256144)  # 256 KB
        # queue.set_attribute('ReceiveMessageWaitTimeSeconds', 0)  # Don't wait
        return queue
