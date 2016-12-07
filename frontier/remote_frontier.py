import json
import errno
import time

import zmq
from zmq.error import ZMQError
from boto.sqs.message import RawMessage as SqsMessage

from python_common.interruptable_thread import InterruptableThread
from atrax.frontier.constants import DEFAULT_FRONTIER_PORT
from atrax.frontier.exceptions import SqsMessageRetentionException


class RemoteFrontier(InterruptableThread):
    def __init__(self, frontier):
        InterruptableThread.__init__(self)
        self._frontier = frontier

    def enqueue(self, request):
        for queue_key, urls in request['urls'].iteritems():
            if len(urls) == 1:
                self._frontier.enqueue(queue_key, urls[0][0], urls[0][1])
            else:
                self._frontier.enqueue_batch(queue_key, urls)

    def dequeue(self, request):
        msg = self._frontier.dequeue(request['fetcher'])
        if msg is None:
            return ''
        elif isinstance(msg, SqsMessage):
            return json.dumps({'msg_id': msg.receipt_handle,
                               'queue': msg.queue.name,
                               'frontier_message': msg.get_body()})
        else:
            return msg

    def run(self):
        self._frontier.start()
        previous_emit_time = time.time()
        request_count = 0

        try:
            zmq_context = zmq.Context.instance()
            zmq_socket = zmq_context.socket(zmq.REP)
            zmq_socket.bind('tcp://*:' + str(DEFAULT_FRONTIER_PORT))

            while not self.is_stop_requested():
                request = zmq_socket.recv_json()
                request_count += 1
                cmd = request['cmd']
                if cmd == 'enqueue':
                    self.enqueue(request)
                    zmq_socket.send('')
                elif cmd == 'dequeue':
                    response = self.dequeue(request)
                    zmq_socket.send_string(response)
                elif cmd == 'enqueue_dequeue':
                    self.enqueue(request)
                    response = self.dequeue(request)
                    zmq_socket.send_string(response)
                elif cmd == 'delete':
                    self._frontier.delete(request['queue'], request['msg_id'])
                    zmq_socket.send('')
                elif cmd == 'emit_metrics':
                    now = time.time()
                    self._frontier.metrics.put_frontier_request_rate(request_count / (now - previous_emit_time))
                    request_count = 0
                    previous_emit_time = now
                    self._frontier.emit_metrics()
                    zmq_socket.send('')
                elif cmd == 'run_maintenance':
                    self._frontier.run_maintenance()
                    zmq_socket.send('')
                elif cmd == 'queue_state':
                    state = request['state']
                    queue_name = request['queue']
                    if state == 'empty':
                        self._frontier.dequeue_failed(queue_name)
                    elif state == 'queue_dieing':
                        raise SqsMessageRetentionException(queue_name)
                    elif state == 'queue_missing':
                        self._frontier.delete_queue(queue_name)
                    zmq_socket.send('')
                elif cmd == 'ping':
                    zmq_socket.send('')
        except ZMQError, ex:
            if ex.errno is None or ex.errno != errno.EINTR:
                raise
