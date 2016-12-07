from waiting import TimeoutExpired
from aws.ec2 import wait_for_state
from atrax.common.exceptions import DependencyInitializationError
from atrax.management.instance_accessor_base import InstanceAccessorBase
from atrax.management.constants import ComputeEnv, InstanceState
from atrax.management.aws_env.instance_accessor import AwsInstanceAccessor
from atrax.management.local_env.instance_accessor import LocalInstanceAccessor


def wait_for_instance(instance, name):
    if instance is None:
        raise DependencyInitializationError(name)

    if instance.state != InstanceState.RUNNING:
        try:
            wait_for_state(instance, [InstanceState.RUNNING])
        except TimeoutExpired:
            raise DependencyInitializationError(name)


class InstanceAccessor(InstanceAccessorBase):
    def __init__(self, crawl_job_name, environment):
        super(InstanceAccessor, self).__init__(crawl_job_name, environment)

        if environment == ComputeEnv.AWS:
            self._instance_accessor = AwsInstanceAccessor(crawl_job_name)
        else:
            self._instance_accessor = LocalInstanceAccessor(crawl_job_name)

        self.crawl_job_name = crawl_job_name
        self.environment = environment

    def get_crawl_job_instances(self):
        return self._instance_accessor.get_crawl_job_instances()

    def get_frontier_instance(self):
        instance = self._instance_accessor.get_frontier_instance()
        wait_for_instance(instance, 'frontier')
        return instance

    def get_redis_instance(self):
        instance = self._instance_accessor.get_redis_instance()
        wait_for_instance(instance, 'redis')
        return instance

    def get_fetcher_instances(self):
        return self._instance_accessor.get_fetcher_instances()

    def get_velum_instance(self):
        instance = self._instance_accessor.get_velum_instance()
        wait_for_instance(instance, 'velum')
        return instance

    def get_instance(self, instance_id):
        return self._instance_accessor.get_instance(instance_id)
