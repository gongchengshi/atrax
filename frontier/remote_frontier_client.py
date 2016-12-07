from collections import defaultdict
import json
from datetime import timedelta
import time

from boto.exception import SQSError
from boto.sqs.message import RawMessage as SqsMessage
import zmq

from aws import USWest2 as AwsConnections
from aws.exceptions import AwsErrorCodes
from atrax.frontier.constants import DEFAULT_FRONTIER_PORT
from atrax.frontier.frontier_interface import FrontierInterface
from atrax.frontier.utils import QueueKeyDict
from atrax.frontier.message import pack_message, unpack_message
from atrax.frontier.exceptions import SqsMessageRetentionException, FrontierNotResponding, FrontierMessageCorrupt
from aws.sqs.message import build_message_shell


class RemoteFrontierClient(FrontierInterface):
    def __init__(self, address, crawl_job_name=None, on_new_queue_assigned=None):
        FrontierInterface.__init__(self)
        self._on_new_queue_assigned = on_new_queue_assigned
        self._zmq_context = zmq.Context.instance()
        self._client = self._zmq_context.socket(zmq.REQ)
        self._client.RCVTIMEO = 1000 * 60  # wait up to a minute for responses to come back
        self._client.connect('tcp://' + address)
        self._sqs = AwsConnections.sqs()
        self._queue_history = {}
        self._queue_names = QueueKeyDict()
        self._messages = {}

    @staticmethod
    def _raise_if_queue_not_ok(message):
        sent_timestamp = int(message.attributes['SentTimestamp'])/1000
        oldest_allowable_timestamp = (time.time() - timedelta(days=13, hours=20).total_seconds())
        if sent_timestamp < oldest_allowable_timestamp:  # the message will expire in less than 4 hours
            raise SqsMessageRetentionException(message.queue)

    def get_or_add_queue(self, queue_name):
        try:
            queue = self._queue_history[queue_name]
        except KeyError:
            queue = self._sqs.lookup(queue_name)
            if queue is None:
                self._client.send_json({'cmd': 'queue_state',
                                       'state': 'queue_missing',
                                       'queue': queue_name})
                self._client.recv()
            else:
                queue.set_message_class(SqsMessage)
                self._queue_history[queue_name] = queue
                if self._on_new_queue_assigned is not None:
                    self._on_new_queue_assigned(queue_name)
        return queue

    def dequeue(self, fetcher_id, msg=None):
        try:
            while True:
                if not msg:  # Setting msg to None signals the loop the request a new queue/message
                    self._client.send_json({'cmd': 'dequeue',
                                           'fetcher': fetcher_id})
                    msg = self._client.recv()
                    if not msg:
                        break

                if msg.startswith('{'):  # This is an actual frontier message
                    message = json.loads(msg)
                    if message is None:
                        raise FrontierMessageCorrupt(msg)

                    msg_id = message['msg_id']
                    queue = self.get_or_add_queue(message['queue'])
                    self._messages[msg_id] = build_message_shell(queue, msg_id)
                    try:
                        return msg_id, unpack_message(message['frontier_message'])
                    except FrontierMessageCorrupt, ex:
                        ex.queue_name = queue.name
                        self.delete(msg_id)
                        raise ex
                else:  # This is a queue name
                    queue = self.get_or_add_queue(msg)
                    if queue is None:
                        msg = None  # Try again
                        continue

                    try:
                        # This doesn't work: message = queue.read(message_attributes=['SentTimestamp'])
                        # see https://github.com/boto/boto/issues/2699.
                        messages = queue.get_messages(attributes=['SentTimestamp'])
                        message = messages[0] if messages else None

                        if message is None:
                            self._client.send_json({'cmd': 'queue_state',
                                                   'state': 'empty',
                                                   'queue': queue.name})
                            self._client.recv()
                            msg = None  # Try again
                        else:
                            self._raise_if_queue_not_ok(message)
                            msg_id = message.receipt_handle
                            self._messages[msg_id] = message
                            body = message.get_body()
                            try:
                                return msg_id, unpack_message(body)
                            except FrontierMessageCorrupt, ex:
                                ex.queue_name = queue.name
                                self.delete(msg_id)
                                raise ex
                    except SqsMessageRetentionException as ex:
                        self._client.send_json({'cmd': 'queue_state',
                                               'state': 'queue_dieing',
                                               'queue': queue.name})
                        self._client.recv()
                    except SQSError as ex:
                        if ex.error_code == AwsErrorCodes.SqsNonExistentQueue:
                            msg = None
                        else:
                            raise
        except zmq.error.Again, ex:
            raise FrontierNotResponding(ex)
        return None, None

    def _pack_url(self, url_info):
        return {self._queue_names[url_info]: [(url_info.id, pack_message(url_info))]}

    def _pack_urls(self, url_infos):
        packed = defaultdict(list)
        for url_info in url_infos:
            packed[self._queue_names[url_info]].append((url_info.id, pack_message(url_info)))
        return packed

    def enqueue(self, url_infos):
        try:
            packed_url_info = self._pack_urls(url_infos)
            self._client.send_json({'cmd': 'enqueue',
                                   'urls': packed_url_info})
            self._client.recv()
        except zmq.error.Again, ex:
            raise FrontierNotResponding(ex)

    def enqueue_dequeue(self, fetcher_id, url_infos):
        try:
            response = None
            if url_infos:
                self._client.send_json({'cmd': 'enqueue_dequeue',
                                       'fetcher': fetcher_id,
                                       'urls': self._pack_urls(url_infos)})
                response = self._client.recv()
            return self.dequeue(fetcher_id, response)
        except zmq.error.Again, ex:
            raise FrontierNotResponding(ex)

    def delete(self, msg_id):
        try:
            self._messages[msg_id].delete()
            del self._messages[msg_id]
            return True
        except KeyError, ex:
            pass
        except AttributeError, ex:
            pass
        return False

    def ping(self):
        orig = self._client.RCVTIMEO
        self._client.RCVTIMEO = 2000
        try:
            self._client.send_json({'cmd': 'ping'})
            self._client.recv()
        except zmq.error.Again:
            return False
        finally:
            self._client.RCVTIMEO = orig

    def reset(self):
        """
        This should be done anytime something other than the frontier adds or deletes queues.
        """
        try:
            self._client.send_json({'cmd': 'reset'})
            self._client.recv()
        except zmq.error.Again, ex:
            raise FrontierNotResponding(ex)


def get_frontier_client(frontier_instance, on_new_queue_assigned):
    return RemoteFrontierClient(frontier_instance.private_ip_address + ':' + str(DEFAULT_FRONTIER_PORT),
                                on_new_queue_assigned=on_new_queue_assigned)
