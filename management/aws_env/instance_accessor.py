from aws import USWest2 as AwsConnections
from atrax.management.constants import ComputeEnv, InstanceState
from atrax.management.instance_accessor_base import InstanceAccessorBase, InstanceInfo
from atrax.management.aws_env.constants import *
from aws.ec2 import on_demand_instance_pricing


class AwsInstanceInfo(InstanceInfo):
    def __init__(self, ec2_instance):
        super(AwsInstanceInfo, self).__init__()
        self.ec2_instance = ec2_instance

    @property
    def state(self):
        return self.ec2_instance.state

    @property
    def placement(self):
        return self.ec2_instance.placement

    @property
    def private_ip_address(self):
        return self.ec2_instance.private_ip_address

    @property
    def id(self):
        return self.ec2_instance.id

    @property
    def cost(self):
        return on_demand_instance_pricing[self.ec2_instance.instance_type]

    @property
    def instance_type(self):
        return self.ec2_instance.instance_type

    def update(self):
        self.ec2_instance.update()

    def start(self):
        self.ec2_instance.start()

    def stop(self):
        self.ec2_instance.stop()

    def terminate(self):
        self.ec2_instance.terminate()


class AwsInstanceAccessor(InstanceAccessorBase):
    def __init__(self, crawl_job_name):
        super(AwsInstanceAccessor, self).__init__(crawl_job_name, ComputeEnv.AWS)
        self.ec2 = AwsConnections.ec2()

    def get_crawl_job_instances(self):
        instances = []
        filters = {'tag:' + CRAWL_JOB_TAG_NAME: self.crawl_job_name}
        for instance in self.ec2.get_only_instances(filters=filters):
            if instance.state not in [InstanceState.TERMINATED, InstanceState.SHUTTING_DOWN]:
                instances.append(AwsInstanceInfo(instance))
        return instances

    def get_crawler_compute_instances(self, instance_type, exclude_spot=False):
        instances = []
        for instance in self.get_crawl_job_instances():
            if instance_type in instance.ec2_instance.tags.get(PACKAGES_TAG_NAME, []):
                instances.append(AwsInstanceInfo(instance))
        return instances

    def get_frontier_instance(self):
        instances = self.get_crawler_compute_instances(ModuleNames.FRONTIER, exclude_spot=True)
        return AwsInstanceInfo(instances[0]) if instances else None

    def get_redis_instance(self):
        instances = self.get_crawler_compute_instances(ModuleNames.REDIS, exclude_spot=True)
        return AwsInstanceInfo(instances[0]) if instances else None

    def get_fetcher_instances(self):
        return self.get_crawler_compute_instances(ModuleNames.FETCHER)

    def get_velum_instance(self):
        for instance in self.ec2.get_only_instances({'spot-instance-request-id': None}):
            if ModuleNames.VELUM in instance.tags.get(PACKAGES_TAG_NAME, []):
                return AwsInstanceInfo(instance)

    def get_instance(self, instance_id):
        reservations = self.ec2.get_all_instances(instance_ids=[instance_id])
        if not reservations:
            return None

        return AwsInstanceInfo(reservations[0].instances[0])
