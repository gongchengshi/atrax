from abc import abstractmethod, ABCMeta, abstractproperty


class InstanceInfo:
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractproperty
    def cost(self):
        pass

    @abstractproperty
    def placement(self):
        pass

    @abstractproperty
    def state(self):
        pass

    @abstractproperty
    def id(self):
        pass

    @abstractproperty
    def private_ip_address(self):
        pass

    @abstractmethod
    def update(self):
        pass


class InstanceAccessorBase:
    __metaclass__ = ABCMeta

    def __init__(self, crawl_job_name, environment):
        self.crawl_job_name = crawl_job_name
        self.environment = environment

    @abstractmethod
    def get_crawl_job_instances(self):
        pass

    @abstractmethod
    def get_frontier_instance(self):
        pass

    @abstractmethod
    def get_redis_instance(self):
        pass

    @abstractmethod
    def get_fetcher_instances(self):
        pass

    @abstractmethod
    def get_velum_instance(self):
        pass

    @abstractmethod
    def get_instance(self, instance_id):
        pass
