import time
from datetime import datetime
from boto.exception import EC2ResponseError

from aws import USWest2 as AwsConnections
from aws.ec2 import on_demand_instance_pricing, VirtualizationType, terminate_spot_instances_by_request, \
    wait_for_state, compute_spot_instance_bid
from atrax.management.aws_env.constants import *
from atrax.management.aws_env.ami import get_latest_fetcher_ami
from atrax.management.aws_env.instance_accessor import AwsInstanceAccessor
from atrax.management.constants import InstanceState
from atrax.management.aws_env.user_data_factory import generate_upstart_script, generate_spot_instance_init_script, \
    create_multipart, generate_stopgap_debian_setup, generate_cloud_config


class FetcherClusterUtil:
    def __init__(self, crawl_job_name):
        self.crawl_job_name = crawl_job_name
        self.ec2 = AwsConnections.ec2()
        self._on_demand_user_data = None
        self._spot_user_data = None

    def get_all_spot_requests(self):
        return self.ec2.get_all_spot_instance_requests(
            filters={'tag:' + CRAWL_JOB_TAG_NAME: self.crawl_job_name})

    def get_unfulfilled_spot_requests(self):
        # This is broken in Boto right now.
        # return self.ec2.get_all_spot_instance_requests(
        # filters={'tag:' + CRAWL_JOB_TAG_NAME: self.crawl_job_name, 'state': 'open'})
        return [r for r in self.get_all_spot_requests() if r.state == 'open']

    def get_open_or_active_spot_requests(self):
        # This is broken in Boto right now
        # return self.ec2.get_all_spot_instance_requests(
        # filters={'tag:' + CRAWL_JOB_TAG_NAME: self.crawl_job_name, 'state': 'open|active'})
        return [r for r in self.get_all_spot_requests() if r.state in {'open', 'active'}]

    def calculate_potential_hourly_cost(self):
        """
        The total of the bids for all spot instances + the cost of any other instances associated with the crawl job.
        """
        instance_accessor = AwsInstanceAccessor(self.crawl_job_name)
        frontier = instance_accessor.get_frontier_instance()
        total = frontier.cost
        seen_urls = instance_accessor.get_redis_instance()
        if seen_urls.id != frontier.id:
            total += seen_urls.cost

        for spot_request in self.get_unfulfilled_spot_requests():
            total += spot_request.price

        for fetcher_instance in instance_accessor.get_fetcher_instances():
            total += fetcher_instance.cost

        return total

    def compute_spot_bid(self, preferred_availability_zone):
        bid = compute_spot_instance_bid(self.ec2, FETCHER_INSTANCE_TYPES, AVAILABILITY_ZONES, preferred_availability_zone)
        ten_percent_more = bid[2] * 1.10  # make the bid 10% higher than the average
        on_demand_instance_price = on_demand_instance_pricing[bid[0]]
        # Don't ever bid more than the on demand price
        bid_price = ten_percent_more if ten_percent_more <= on_demand_instance_price else on_demand_instance_price
        return bid[0], bid[1], bid_price

    @property
    def spot_user_data(self):
        if self._spot_user_data is None:
            self._spot_user_data = create_multipart([
                ('spot_instance_init.sh', 'x-shellscript',
                    generate_spot_instance_init_script(self.crawl_job_name, ["fetcher"])),
                ('cloud_config.yaml', 'cloud-config', generate_cloud_config()),
                ('stopgap_debian_setup.sh', 'x-shellscript', generate_stopgap_debian_setup()),
                ('fetcher.conf', 'upstart-job', generate_upstart_script(self.crawl_job_name, "fetcher"))
            ])
        return self._spot_user_data

    def request_spot_instances(self, instance_type, availability_zone, bid_price, count, expires):
        virtualization_type = VirtualizationType.PARAVIRTUAL if instance_type.startswith(
            'm1') else VirtualizationType.HVM

        image_id = get_latest_fetcher_ami(self.ec2, virtualization_type).id
        requests = self.ec2.request_spot_instances(bid_price, image_id, count, instance_type=instance_type,
                                                   valid_until=datetime.utcfromtimestamp(expires).isoformat() if expires else None,
                                                   key_name=EC2_KEY_PAIR_NAME,
                                                   security_groups=[FETCHER_SECURITY_GROUP_NAME],
                                                   placement=availability_zone,
                                                   availability_zone_group=availability_zone,
                                                   instance_profile_arn=SPOT_INSTANCE_ARN,
                                                   user_data=self.spot_user_data)

        request_ids = [request.id for request in requests]

        while True:
            try:
                found_requests = self.ec2.get_all_spot_instance_requests(request_ids)
                if len(found_requests) >= len(request_ids):
                    break
            except EC2ResponseError:
                pass

            time.sleep(1)

        self.ec2.create_tags(request_ids, {CRAWL_JOB_TAG_NAME: self.crawl_job_name})

    def terminate_fetcher_instances(self):
        requests = self.get_all_spot_requests()
        request_ids = [request.id for request in requests]
        terminate_spot_instances_by_request(AwsConnections.ec2(), request_ids)

        for fetcher_instance in AwsInstanceAccessor(self.crawl_job_name).get_fetcher_instances():
            fetcher_instance.terminate()

    @property
    def on_demand_user_data(self):
        if self._on_demand_user_data is None:
            self._on_demand_user_data = create_multipart([
                ('cloud_config.yaml', 'cloud-config', generate_cloud_config()),
                ('stopgap_debian_setup.sh', 'x-shellscript', generate_stopgap_debian_setup()),
                ('fetcher.conf', 'upstart-job', generate_upstart_script(self.crawl_job_name, "fetcher"))
            ])
        return self._on_demand_user_data

    def start_on_demand_instances(self, count, availability_zone):
        modules = [ModuleNames.FETCHER]
        security_groups = [FETCHER_SECURITY_GROUP_NAME]

        fetcher_instances = []

        for i in xrange(0, count):
            reservation = self.ec2.run_instances(image_id=get_latest_fetcher_ami(self.ec2, VirtualizationType.HVM).id,
                                                 key_name=EC2_KEY_PAIR_NAME, security_groups=security_groups,
                                                 instance_type=FETCHER_INSTANCE_TYPES[0], placement=availability_zone,
                                                 monitoring_enabled=True,
                                                 instance_initiated_shutdown_behavior='terminate',
                                                 instance_profile_arn=STANDARD_INSTANCE_ARN,
                                                 user_data=self.on_demand_user_data)

            fetcher_instance = reservation.instances[0]
            wait_for_state(fetcher_instance, (InstanceState.PENDING, InstanceState.RUNNING))
            self.ec2.create_tags([fetcher_instance.id],
                                 {CRAWL_JOB_TAG_NAME: self.crawl_job_name,
                                  PACKAGES_TAG_NAME: ' '.join(modules)})

            cloudwatch = AwsConnections.cloudwatch()
            alarm = cloudwatch.MetricAlarm(name=fetcher_instance.id + '-LOW_NETWORK',
                                           description="Stop the instance when network in drops below 300 bytes in 5 minutes.",
                                           namespace='AWS/EC2', dimensions={'InstanceId': [fetcher_instance.id]},
                                           metric='NetworkIn', statistic='Sum', comparison='<', threshold=300,
                                           period=300, evaluation_periods=1)
            alarm.add_alarm_action('arn:aws:automate:' + AwsConnections.region + ':ec2:terminate')
            cloudwatch.put_metric_alarm(alarm)
            fetcher_instances.append(fetcher_instance)

        return fetcher_instances
