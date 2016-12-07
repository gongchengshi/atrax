from abc import abstractmethod, ABCMeta


class FrontierInterface:
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    def dequeue(self, fetcher_id):
        pass

    @abstractmethod
    def enqueue(self, url_infos):
        pass

    @abstractmethod
    def enqueue_dequeue(self, fetcher_id, url_infos):
        pass

    @abstractmethod
    def delete(self, msg_id):
        pass
