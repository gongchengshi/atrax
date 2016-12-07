from atrax.common.constants import LOCALHOST_IP
from atrax.management.constants import ComputeEnv, InstanceState
from atrax.management.instance_accessor_base import InstanceAccessorBase, InstanceInfo


class LocalInstanceInfo(InstanceInfo):
    def __init__(self, instance_id='0000'):
        InstanceInfo.__init__(self)
        self._state = InstanceState.PENDING
        self._instance_id = instance_id

    @property
    def cost(self):
        return 0.0

    @property
    def id(self):
        return self._instance_id

    @property
    def state(self):
        return self._state

    @property
    def placement(self):
        return ''

    @property
    def private_ip_address(self):
        return LOCALHOST_IP

    def update(self):
        pass


class LocalInstanceAccessor(InstanceAccessorBase):
    processes = []

    def __init__(self, crawl_job_name):
        super(LocalInstanceAccessor, self).__init__(crawl_job_name, ComputeEnv.LOCAL)

    def get_crawl_job_instances(self):
        inst = LocalInstanceInfo()
        inst._state = InstanceState.RUNNING
        return [inst]

    def get_frontier_instance(self):
        inst = LocalInstanceInfo()
        inst._state = InstanceState.RUNNING
        return inst

    def get_redis_instance(self):
        inst = LocalInstanceInfo()
        inst._state = InstanceState.RUNNING
        return inst

    def get_fetcher_instances(self):
        fetcher_instances = []
        for i in xrange(0, len(LocalInstanceAccessor.processes)):
            p = LocalInstanceAccessor.processes[i]
            inst = LocalInstanceInfo(str(i))
            inst._state = InstanceState.RUNNING if p.is_alive() else InstanceState.TERMINATED
            fetcher_instances.append(inst)
        return fetcher_instances

    def get_velum_instance(self):
        inst = LocalInstanceInfo()
        inst._state = InstanceState.RUNNING
        return inst

    def get_instance(self, instance_id):
        inst = LocalInstanceInfo(instance_id)
        if instance_id < len(LocalInstanceAccessor.processes):
            p = LocalInstanceAccessor.processes[int(instance_id)]
            inst._state = InstanceState.RUNNING if p.is_alive() else InstanceState.TERMINATED
        else:
            inst._state = InstanceState.TERMINATED

        return inst
