from aws import USWest2 as AwsConnections
from atrax.frontier.frontier_interface import FrontierInterface


class SqsFrontier(FrontierInterface):
    def __init__(self, queue_name):
        FrontierInterface.__init__(self)

        self.queue = AwsConnections.sqs().lookup(queue_name)

    def dequeue(self, dummy):
        messages = self.queue.get_messages(1, wait_time_seconds=0)
        if len(messages) == 0:
            return None, None

        return messages[0], messages[0].messages[0].get_body()

    def enqueue(self, url_infos):
        for url_info in url_infos:
            print "Attempting to enqueue: " + url_info.url

    def enqueue_dequeue(self, fetcher_id, url_infos):
        self.enqueue(url_infos)
        return self.dequeue(fetcher_id)

    def delete(self, msg):
        msg.delete()
