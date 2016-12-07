from atrax.common.url_info import UrlInfo
from atrax.frontier.frontier_interface import FrontierInterface


class FileFrontier(FrontierInterface):
    def __init__(self, location):
        FrontierInterface.__init__(self)

        self.file = open(location, 'r')
        self.cur = 0

    def dequeue(self, fetcher_id):
        line = self.file.readline()
        if line:
            return self.cur, UrlInfo(line)
        else:
            return None, None

    def enqueue(self, url_infos):
        for url_info in url_infos:
            print "Attempted enqueue of: " + url_info.url

    def enqueue_dequeue(self, fetcher_id, url_infos):
        self.enqueue(url_infos)
        return self.dequeue(fetcher_id)

    def delete(self, msg_id):
        self.cur += 1
